package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	"github.com/google/go-github/v72/github"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sqlbunny/errors"
)

// jobWorker continuously processes jobs from the queue
func (s *Service) jobWorker() {
	for {
		job := s.jobQueue.Dequeue()

		queuedJobs := s.jobQueue.Len()
		s.runningJobsMutex.Lock()
		runningJobs := len(s.runningJobs)
		s.runningJobsMutex.Unlock()

		log.Printf("Starting job %s (%s) [Priority: %d] - Queue: %d jobs, Running: %d/%d",
			job.ID, job.Name, job.Priority, queuedJobs, runningJobs, s.config.MaxConcurrency)

		s.runJob(context.Background(), job)

		s.runningJobsMutex.Lock()
		delete(s.runningJobs, job.ID)
		s.runningJobsMutex.Unlock()

		log.Printf("Finished job %s (%s)", job.ID, job.Name)
	}
}

// enqueueJob adds a job to the queue
func (s *Service) enqueueJob(job *Job) {
	queuedJobs, runningJobs := s.getQueueStatus()
	log.Printf("Enqueuing job %s (%s) [Priority: %d, Dedup: %s] - Queue: %d jobs, Running: %d/%d",
		job.ID, job.Name, job.Priority, job.Dedup, queuedJobs, runningJobs, s.config.MaxConcurrency)

	// Report status to GitHub that job is being processed
	gh, err := s.githubClient(job.InstallationID)
	if err != nil {
		log.Printf("error creating github client for status update: %v", err)
	} else {
		err = s.setStatus(context.Background(), gh, job, "pending", "Job enqueued")
		if err != nil {
			log.Printf("error setting enqueued status: %v", err)
		}
	}

	// Handle deduplication
	if job.Dedup != DedupNone {
		dedupKey := job.DedupKey()

		// Remove queued jobs with the same dedup key
		s.jobQueue.RemoveByDedupKey(dedupKey)

		// Handle running jobs based on dedup mode
		switch job.Dedup {
		case DedupKill:
			s.runningJobsMutex.Lock()
			if runningJob, exists := s.runningJobsByDedup[dedupKey]; exists {
				// Signal the running job to be cancelled
				runningJob.Cancel()
				log.Printf("Killing running job %s due to deduplication", runningJob.ID)
			}
			s.runningJobsMutex.Unlock()
		case DedupDequeue:
			// For dequeue mode, check if there's a running job with the same dedup key
			s.runningJobsMutex.Lock()
			if runningJob, exists := s.runningJobsByDedup[dedupKey]; exists {
				// There's a running job, add this job to the waiting list
				s.waitingJobs[dedupKey] = append(s.waitingJobs[dedupKey], job)
				s.runningJobsMutex.Unlock()
				log.Printf("Job %s (%s) is waiting for running job %s to finish (dedup key: %s)",
					job.ID, job.Name, runningJob.ID, dedupKey)

				// Report waiting status to GitHub
				if gh != nil {
					err = s.setStatus(context.Background(), gh, job, "pending", fmt.Sprintf("Waiting for job %s to finish", runningJob.ID))
					if err != nil {
						log.Printf("error setting waiting status: %v", err)
					}
				}
				return // Don't enqueue the job yet
			}
			s.runningJobsMutex.Unlock()
			log.Printf("No running duplicate found for dedup key: %s, proceeding with job %s", dedupKey, job.ID)
		}
	}

	s.jobQueue.Enqueue(job)
}

// enqueueWaitingJobs checks for jobs waiting for the given dedup key and enqueues them
func (s *Service) enqueueWaitingJobs(dedupKey string) {
	if dedupKey == "" {
		return
	}

	s.runningJobsMutex.Lock()
	waitingJobs := s.waitingJobs[dedupKey]
	delete(s.waitingJobs, dedupKey)
	s.runningJobsMutex.Unlock()

	for _, waitingJob := range waitingJobs {
		log.Printf("Enqueuing previously waiting job %s (%s) for dedup key: %s",
			waitingJob.ID, waitingJob.Name, dedupKey)

		// Report status update when moving from waiting to queued
		gh, err := s.githubClient(waitingJob.InstallationID)
		if err != nil {
			log.Printf("error creating github client for status update: %v", err)
		} else {
			err = s.setStatus(context.Background(), gh, waitingJob, "pending", "Job enqueued")
			if err != nil {
				log.Printf("error setting queued status for waiting job: %v", err)
			}
		}

		s.jobQueue.Enqueue(waitingJob)
	}
}

// getQueueStatus returns the current queue status
func (s *Service) getQueueStatus() (queuedJobs int, runningJobs int) {
	s.runningJobsMutex.Lock()
	runningJobs = len(s.runningJobs)
	s.runningJobsMutex.Unlock()

	queuedJobs = s.jobQueue.Len()
	return queuedJobs, runningJobs
}

func (s *Service) isJobRunning(id string) bool {
	s.runningJobsMutex.Lock()
	_, isRunning := s.runningJobs[id]
	s.runningJobsMutex.Unlock()
	return isRunning
}

func (s *Service) setStatus(ctx context.Context, gh *github.Client, j *Job, state string, description string) error {
	url := fmt.Sprintf("%s/jobs/%s", s.config.ExternalURL, j.ID)
	_, _, err := gh.Repositories.CreateStatus(ctx,
		*j.Repo.Owner.Login,
		*j.Repo.Name,
		j.SHA,
		&github.RepoStatus{
			State:       github.Ptr(state),
			Context:     github.Ptr(fmt.Sprintf("ci/%s", j.Name)),
			Description: github.Ptr(description),
			TargetURL:   &url,
		})
	return err
}

func (s *Service) runJob(ctx context.Context, job *Job) {
	// Create a cancellable context for this job
	jobCtx, cancel := context.WithCancel(ctx)

	// Set the cancel function and start time in the job
	job.cancelFunc = cancel
	job.StartTime = time.Now()

	s.runningJobsMutex.Lock()
	s.runningJobs[job.ID] = job
	// Track by dedup key if deduplication is enabled
	if job.Dedup != DedupNone {
		s.runningJobsByDedup[job.DedupKey()] = job
	}
	s.runningJobsMutex.Unlock()

	defer func() {
		var dedupKey string
		s.runningJobsMutex.Lock()
		delete(s.runningJobs, job.ID)
		if job.Dedup != DedupNone {
			dedupKey = job.DedupKey()
			delete(s.runningJobsByDedup, dedupKey)
		}
		s.runningJobsMutex.Unlock()

		// Check for waiting jobs after releasing the mutex
		if dedupKey != "" {
			s.enqueueWaitingJobs(dedupKey)
		}
	}()

	logs, err := os.Create(filepath.Join(s.config.DataDir, "logs", job.ID))
	if err != nil {
		log.Printf("error creating log file: %v", err)
		return
	}

	gh, err := s.githubClient(job.InstallationID)
	if err != nil {
		log.Printf("error creating github client: %v", err)
		return
	}

	err = s.setStatus(ctx, gh, job, "pending", "Job is running...")
	if err != nil {
		log.Printf("error creating pending status: %v", err)
	}

	err = nopanic(func() error {
		return s.runJobInner(jobCtx, job, gh, logs)
	})

	result := "success"
	description := "Job completed successfully"
	if err != nil {
		fmt.Fprintf(logs, "run failed: %v\n", err)
		log.Printf("job run failed: %v", err)
		result = "failure"
		description = fmt.Sprintf("Job failed: %v", err)
	}

	err = s.setStatus(ctx, gh, job, result, description)
	if err != nil {
		log.Printf("error creating result status: %v", err)
	}
}

func (s *Service) runJobInner(ctx context.Context, job *Job, gh *github.Client, logs *os.File) error {
	token, err := s.getRepoToken(ctx, job)
	if err != nil {
		return err
	}
	log.Printf("repo token: %s", token)

	ctx = namespaces.WithNamespace(ctx, "bender")

	image, err := s.containerd.GetImage(ctx, s.config.Image)
	if err != nil {
		log.Println("Image not found. pulling it. ", err)
		image, err = s.containerd.Pull(ctx, s.config.Image, containerd.WithPullUnpack)
		if err != nil {
			return err
		}
	}

	// Read image imageConfig.
	var imageConfig ocispec.Image
	configDesc, err := image.Config(ctx) // aware of img.platform
	if err != nil {
		return err
	}
	p, err := content.ReadBlob(ctx, image.ContentStore(), configDesc)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(p, &imageConfig); err != nil {
		return err
	}

	log.Println("creating container")

	// Create job dir
	jobDir := filepath.Join(s.config.DataDir, "jobs", job.ID)
	err = os.MkdirAll(jobDir, 0700)
	if err != nil {
		return err
	}
	home := filepath.Join(jobDir, "home")
	err = os.MkdirAll(home, 0700)
	if err != nil {
		return err
	}
	defer func() {
		log.Printf("deleting job dir: %s", jobDir)
		err := os.RemoveAll(jobDir)
		if err != nil {
			log.Printf("error deleting job dir: %v", err)
		}
	}()

	// Setup cache
	cacheDir := filepath.Join(s.config.DataDir, "cache", *job.Repo.Owner.Login, *job.Repo.Name, job.Name)
	err = os.MkdirAll(cacheDir, 0700)
	if err != nil {
		return err
	}

	cacheBaseName := ""
	for _, cache := range job.Cache {
		log.Printf("checking cache %s", cache)
		if stat, err := os.Stat(filepath.Join(cacheDir, cache)); err == nil && stat.IsDir() {
			cacheSize, err := dirSize(filepath.Join(cacheDir, cache))
			if err != nil {
				log.Printf("failed to calc cache size: %v", err)
				continue
			}

			log.Printf("cache %s size: %d MB", cache, cacheSize/1024/1024)
			if cacheSize > int64(s.config.Cache.MaxSizeMB)*1024*1024 {
				log.Printf("cache %s too big, ignoring it", cache)
			}

			cacheBaseName = cache
			break
		} else {
			log.Printf("cache %s not found", cache)

		}
	}
	jobCacheDir := filepath.Join(jobDir, "cache")
	if cacheBaseName == "" {
		log.Printf("no base cache found")
		err = doExec("btrfs", "subvolume", "create", jobCacheDir)
	} else {
		log.Printf("using base cache %s", cacheBaseName)
		baseCacheDir := filepath.Join(cacheDir, cacheBaseName)

		// Touch base cache, to let cache GC know it's recently used.
		now := time.Now().Local()
		err = os.Chtimes(baseCacheDir, now, now)
		if err != nil {
			return err
		}
		err = doExec("btrfs", "subvolume", "snapshot", baseCacheDir, jobCacheDir)
	}
	if err != nil {
		return err
	}
	defer func() {
		if _, err := os.Stat(jobCacheDir); err == nil {
			log.Printf("deleting cache %s", jobCacheDir)
			err := doExec("btrfs", "subvolume", "delete", jobCacheDir)
			if err != nil {
				log.Printf("error deleting cache: %v", err)
			}
		}
	}()

	// Setup home dir
	jobArtifactsDir := filepath.Join(home, "artifacts")
	err = os.MkdirAll(jobArtifactsDir, 0700)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString("machine github.com\nlogin x-access-token\npassword ")
	buf.WriteString(token)
	err = os.WriteFile(filepath.Join(home, ".netrc"), buf.Bytes(), 0600)
	if err != nil {
		return err
	}

	buf = bytes.NewBuffer(nil)
	buf.WriteString(`
[user]
email = ci@embassy.dev
name = Embassy CI
[init]
defaultBranch = main
[advice]
detachedHead = false
`)
	err = os.WriteFile(filepath.Join(home, ".gitconfig"), buf.Bytes(), 0600)
	if err != nil {
		return err
	}

	j, err := json.Marshal(job)
	if err != nil {
		return err
	}
	err = os.WriteFile(filepath.Join(home, "job.json"), j, 0600)
	if err != nil {
		return err
	}

	buf = bytes.NewBuffer(nil)
	buf.WriteString("#!/bin/bash\n")
	buf.WriteString("set -euxo pipefail\n")
	buf.WriteString(fmt.Sprintf("git clone -n %s code\n", job.CloneURL))
	buf.WriteString("cd code\n")
	buf.WriteString(fmt.Sprintf("git checkout %s\n", job.SHA))
	buf.WriteString(fmt.Sprintf("exec %s\n", job.Script))
	err = os.WriteFile(filepath.Join(home, "entrypoint.sh"), buf.Bytes(), 0700)
	if err != nil {
		return err
	}

	mounts := []specs.Mount{
		{
			Type:        "none",
			Source:      home,
			Destination: "/ci",
			Options:     []string{"rbind"},
		},
		{
			Type:        "none",
			Source:      jobCacheDir,
			Destination: "/ci/cache",
			Options:     []string{"rbind"},
		},
	}

	if s.config.NetSandbox != nil {
		mounts = append(mounts, specs.Mount{
			Type:        "none",
			Source:      filepath.Join(s.config.DataDir, "resolv.conf"),
			Destination: "/etc/resolv.conf",
			Options:     []string{"rbind", "ro"},
		})
	} else {
		mounts = append(mounts, specs.Mount{
			Type:        "none",
			Source:      "/etc/resolv.conf",
			Destination: "/etc/resolv.conf",
			Options:     []string{"rbind", "ro"},
		})
	}

	if job.Trusted {
		secretPath := filepath.Join(s.config.DataDir, "secrets", *job.Repo.Owner.Login, *job.Repo.Name)
		err = os.MkdirAll(secretPath, 0700)
		if err != nil {
			return err
		}

		mounts = append(mounts, specs.Mount{
			Type:        "none",
			Source:      secretPath,
			Destination: "/ci/secrets",
			Options:     []string{"rbind"},
		})
	}

	// setup cgroup
	jobCGroup, err := s.cgroup.CreateJobCgroup(job.ID)
	if err != nil {
		return err
	}

	err = jobCGroup.SetValue("memory.oom.group", "1")
	if err != nil {
		log.Printf("Warning: failed to set memory.oom.group=1 for job %s: %v", job.ID, err)
		// Don't fail the job if we can't set this - it's not critical
	}

	jobName := fmt.Sprintf("job-%s", job.ID)

	container, err := s.containerd.NewContainer(ctx, jobName,
		containerd.WithNewSnapshot(fmt.Sprintf("job-%s-rootfs", job.ID), image),
		containerd.WithNewSpec(
			oci.WithProcessArgs("/bin/bash", "-c", "./entrypoint.sh 2>&1"),
			oci.WithProcessCwd("/ci"),
			oci.WithUIDGID(1000, 1000),
			oci.WithDefaultPathEnv,
			oci.WithEnv(imageConfig.Config.Env),
			oci.WithEnv([]string{
				"HOME=/ci",
				"GITHUB_TOKEN=" + token,
			}),
			oci.WithCgroup(jobCGroup.Path),
			oci.WithHostNamespace(specs.NetworkNamespace), // TODO network sandboxing
			oci.WithMounts(mounts),
		),
	)
	if err != nil {
		return err
	}
	defer container.Delete(ctx)

	log.Println("creating task")

	// create a new task
	task, err := container.NewTask(ctx, cio.NewCreator(
		cio.WithFIFODir(filepath.Join(s.config.DataDir, "fifo")),
		cio.WithStreams(nil, logs, logs),
	))
	if err != nil {
		return err
	}
	defer task.Delete(ctx)
	defer task.Kill(ctx, syscall.SIGKILL)

	// the task is now running and has a pid that can be used to setup networking
	// or other runtime settings outside of containerd
	pid := task.Pid()
	log.Printf("pid: %d", pid)

	log.Println("starting task")

	// start the process inside the container
	err = task.Start(ctx)
	if err != nil {
		return err
	}

	// wait for the task to exit and get the exit status
	statusC, err := task.Wait(ctx)
	if err != nil {
		return err
	}

	status := <-statusC

	// Commit cache
	primary := job.Cache[0]
	log.Printf("committing cache to primary %s", primary)
	primaryPath := filepath.Join(cacheDir, primary)
	if _, err := os.Stat(primaryPath); err == nil {
		err = doExec("btrfs", "subvolume", "delete", primaryPath)
		if err != nil {
			log.Printf("failed to remove old primary cache %s: %v. Trying `rm -rf`", primaryPath, err)
			err = os.RemoveAll(primaryPath)
			if err != nil {
				log.Printf("failed to remove old primary cache %s with `rm -rf`: %v", primaryPath, err)
			}
		}
	}

	err = os.Rename(jobCacheDir, primaryPath)
	if err != nil {
		log.Printf("failed to rename cache %s to %s: %v", jobCacheDir, primaryPath, err)
	}

	// Sanitize and publish artifacts
	err = removeSymlinks(jobArtifactsDir)
	if err != nil {
		log.Printf("failed to remove symlinks in artifact dir: %v", err)
	} else {
		artifactsDir := filepath.Join(s.config.DataDir, "artifacts", job.ID)
		err = os.Rename(jobArtifactsDir, artifactsDir)
		if err != nil {
			log.Printf("failed to rename artifact dir: %v", err)
		}
	}

	// Post github comment
	err = s.postComment(ctx, job, gh, home)
	if err != nil {
		log.Printf("failed to post github comment: %v", err)
	}

	if err := status.Error(); err != nil {
		return err
	}
	if status.ExitCode() != 0 {
		return errors.Errorf("exited with code %d", status.ExitCode())
	}
	return nil
}

func (s *Service) postComment(ctx context.Context, job *Job, gh *github.Client, home string) error {
	if job.PullRequest == nil {
		return nil
	}

	commentPath := filepath.Join(home, "comment.md")
	stat, err := os.Lstat(commentPath)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	if stat.IsDir() || stat.Mode()&os.ModeSymlink == os.ModeSymlink {
		return nil
	}

	comment, err := os.ReadFile(commentPath)
	if err != nil {
		return err
	}

	// post comment to github
	_, _, err = gh.Issues.CreateComment(ctx, *job.Repo.Owner.Login, *job.Repo.Name, *job.PullRequest.Number, &github.IssueComment{
		Body: github.Ptr(string(comment)),
	})
	if err != nil {
		return err
	}

	return nil
}

// recursively remove all symlinks in a directory
func removeSymlinks(path string) error {
	return filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if info.Mode()&os.ModeSymlink != os.ModeSymlink {
			return nil
		}
		return os.Remove(path)
	})
}

func (s *Service) getRepoToken(ctx context.Context, job *Job) (string, error) {
	var permissions = github.InstallationPermissions{
		Metadata: github.Ptr("read"),
		Contents: github.Ptr("read"),
	}
	var repositories = []string{
		*job.Repo.Name,
	}

	if job.Trusted {
		for key, value := range job.Permissions {
			if value != "read" && value != "write" {
				return "", errors.Errorf("invalid permission %q for %q", value, key)
			}

			switch key {
			case "actions":
				permissions.Actions = github.Ptr(value)
			case "checks":
				permissions.Checks = github.Ptr(value)
			case "contents":
				permissions.Contents = github.Ptr(value)
			case "deployments":
				permissions.Deployments = github.Ptr(value)
			case "issues":
				permissions.Issues = github.Ptr(value)
			case "packages":
				permissions.Packages = github.Ptr(value)
			case "pages":
				permissions.Pages = github.Ptr(value)
			case "pull_requests":
				permissions.PullRequests = github.Ptr(value)
			case "repository_projects":
				permissions.RepositoryProjects = github.Ptr(value)
			case "security_events":
				permissions.SecurityEvents = github.Ptr(value)
			case "statuses":
				permissions.Statuses = github.Ptr(value)
			default:
				return "", errors.Errorf("Unknown permission: %q", key)
			}
		}

		repositories = append(repositories, job.PermissionRepos...)
	}

	itr, err := ghinstallation.New(http.DefaultTransport, s.config.Github.AppID, job.InstallationID, []byte(s.config.Github.PrivateKey))
	itr.InstallationTokenOptions = &github.InstallationTokenOptions{
		Permissions:  &permissions,
		Repositories: repositories,
	}

	if err != nil {
		return "", errors.Errorf("Failed to create ghinstallation: %w", err)
	}

	token, err := itr.Token(ctx)
	if err != nil {
		return "", errors.Errorf("Failed to get repo token: %w", err)
	}

	return token, nil
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
