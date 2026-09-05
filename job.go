package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/google/go-github/v88/github"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sqlbunny/errors"
)

// withOOMScoreAdj sets the process oom_score_adj. A positive value makes the
// job's processes the preferred victims of the kernel's global (system-wide)
// OOM killer, so that when the whole host runs out of memory, job processes are
// killed before bender or other system services.
func withOOMScoreAdj(adj int) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
		if s.Process == nil {
			s.Process = &specs.Process{}
		}
		s.Process.OOMScoreAdj = &adj
		return nil
	}
}

type Event struct {
	Event      string            `json:"event"`
	Attributes map[string]string `json:"-"`

	Repo           *github.Repository  `json:"repository"`
	PullRequest    *github.PullRequest `json:"pull_request"`
	CloneURL       string              `json:"-"`
	SHA            string              `json:"-"`
	InstallationID int64               `json:"-"`

	// Cache[0] is the primary cache, Cache[1:] are secondary caches
	// that will be cloned into the primary cache if the primary cache
	// does not exist.
	// Example for PR 1234, which targets the foo branch:
	//    "pr-1234", "branch-foo", "branch-main"
	Cache []string `json:"-"`

	// If true, secrets will be mounted.
	Trusted bool `json:"-"`
}

// JobState represents the current state of a job
type JobState int

const (
	JobStateQueued  JobState = iota // Job is queued and waiting to be started
	JobStateRunning                 // Job is currently running
)

func (s JobState) String() string {
	switch s {
	case JobStateQueued:
		return "queued"
	case JobStateRunning:
		return "running"
	default:
		return "unknown"
	}
}

type Job struct {
	*Event
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Priority        int               `json:"priority"`
	Dedup           DedupMode         `json:"-"` // Deduplication mode
	Cooldown        time.Duration     `json:"-"` // Cooldown duration before job can start
	Script          string            `json:"-"`
	Permissions     map[string]string `json:"-"`
	PermissionRepos []string          `json:"-"`

	// Internal fields for job management
	State           JobState           `json:"-"` // Current state of the job
	EnqueuedAt      time.Time          `json:"-"` // When the job was enqueued
	StartedAt       time.Time          `json:"-"` // When the job started running
	RunnableAt      time.Time          `json:"-"` // When the job became runnable (for cooldown)
	CooldownReadyAt time.Time          `json:"-"` // When the cooldown expires and job can actually start
	cancelFunc      context.CancelFunc `json:"-"` // Function to cancel this job
	cancelReason    string             `json:"-"` // Why this job was canceled, if it was. Guarded by the queue mutex.
}

// Cancel cancels the job if it's running
func (j *Job) Cancel() {
	if j.cancelFunc != nil {
		j.cancelFunc()
	}
}

// DedupKey generates a unique deduplication key for this job
func (j *Job) DedupKey() string {
	if j.Dedup == DedupNone {
		return "" // No deduplication
	}

	// Handle nil fields for tests
	if j.Repo == nil || j.Repo.Owner == nil || j.Repo.Owner.Login == nil || j.Repo.Name == nil {
		return fmt.Sprintf("test/%s", j.Name) // Fallback for tests
	}

	// Base key: owner/repo/jobname
	key := fmt.Sprintf("%s/%s/%s", *j.Repo.Owner.Login, *j.Repo.Name, j.Name)

	// Add branch or PR number
	if j.PullRequest != nil && j.PullRequest.Number != nil {
		key += fmt.Sprintf("/pr-%d", *j.PullRequest.Number)
	} else if branch, ok := j.Attributes["branch"]; ok {
		key += fmt.Sprintf("/branch-%s", branch)
	}

	return key
}

func (s *Service) setStatus(ctx context.Context, gh *github.Client, j *Job, state string, description string) error {
	url := fmt.Sprintf("%s/jobs/%s", s.config.ExternalURL, j.ID)
	_, _, err := gh.Repositories.CreateStatus(ctx,
		*j.Repo.Owner.Login,
		*j.Repo.Name,
		j.SHA,
		github.RepoStatus{
			State:       github.Ptr(state),
			Context:     github.Ptr(fmt.Sprintf("ci/%s", j.Name)),
			Description: github.Ptr(description),
			TargetURL:   &url,
		})
	return err
}

// postCancelStatuses reports a terminal GitHub status for jobs that were
// canceled while still queued. A running job reports its own status when the
// canceled context unwinds runJob, but a queued one never started, so without
// this GitHub would show it as pending forever.
func (s *Service) postCancelStatuses(jobs []*Job, reason string) {
	for _, job := range jobs {
		gh, err := s.githubClient(job.InstallationID)
		if err != nil {
			log.Printf("error creating github client for cancel status: %v", err)
			continue
		}
		err = s.setStatus(context.Background(), gh, job, "failure", fmt.Sprintf("Canceled: %s", reason))
		if err != nil {
			log.Printf("error setting cancel status for job %s: %v", job.ID, err)
		}
	}
}

func (s *Service) runJob(ctx context.Context, job *Job) {
	// Create a cancellable context for this job
	jobCtx, cancel := context.WithCancel(ctx)

	// Set the cancel function in the job
	job.cancelFunc = cancel

	defer func() {
		// Call onJobFinished when the job completes
		s.queue.onJobFinished(job)
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

	// Keep GitHub posted while the job runs. A job lasting more than an hour
	// would otherwise go silent and hit GitHub's merge-queue check timeout
	// (default 60 minutes), failing the merge group even though the job is
	// perfectly healthy.
	if interval := s.config.StatusRefreshInterval; interval > 0 {
		done := make(chan struct{})
		defer close(done)
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					desc := fmt.Sprintf("Job is running... (%s so far)", formatDuration(time.Since(job.StartedAt)))
					if err := s.setStatus(ctx, gh, job, "pending", desc); err != nil {
						log.Printf("error refreshing running status for job %s: %v", job.ID, err)
					}
				}
			}
		}()
	}

	err = nopanic(func() error {
		return s.runJobInner(jobCtx, job, gh, logs)
	})

	duration := formatDuration(time.Since(job.StartedAt))

	result := "success"
	description := fmt.Sprintf("Completed in %s", duration)
	if err != nil {
		log.Printf("job run failed: %v", err)
		result = "failure"
		if reason := s.queue.cancelReason(job); reason != "" {
			// Say why in the log too. Otherwise all the log shows is whatever
			// error the cancellation happened to surface ("context canceled"),
			// which doesn't tell the reader anyone canceled it, let alone who.
			fmt.Fprintf(logs, "job canceled: %s\n", reason)
			description = fmt.Sprintf("Canceled after %s: %s", duration, reason)
		} else {
			fmt.Fprintf(logs, "run failed: %v\n", err)
			// The duration goes before the error: GitHub truncates status
			// descriptions at 140 chars and the error can be arbitrarily long.
			description = fmt.Sprintf("Failed in %s: %v", duration, err)
		}
	}

	fmt.Fprintf(logs, "job %s in %s\n", result, duration)

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

	// Create job dir as btrfs subvolume
	jobDir := filepath.Join(s.config.DataDir, "jobs", job.ID)
	err = exec.Command("btrfs", "subvolume", "create", jobDir).Run()
	if err != nil {
		return fmt.Errorf("btrfs subvolume create: %w", err)
	}
	home := filepath.Join(jobDir, "home")
	err = os.MkdirAll(home, 0700)
	if err != nil {
		return err
	}
	defer func() {
		log.Printf("deleting job dir: %s", jobDir)
		err := exec.Command("btrfs", "subvolume", "delete", jobDir).Run()
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

	// Set a per-job memory limit with no swap. This is what makes
	// memory.oom.group actually do its job: memory.oom.group only triggers a
	// group-kill on a *cgroup-level* OOM, i.e. when this cgroup hits its own
	// memory.max. Without a per-job memory.max, a runaway job instead spills
	// into swap and/or trips the global OOM killer, which kills a single
	// process (ignoring oom.group) and can take down the whole host. With a
	// per-job limit + swap disabled, exceeding it triggers an immediate cgroup
	// OOM on this exact cgroup, and oom.group=1 kills the whole job tree
	// atomically.
	if err := jobCGroup.SetValue("memory.swap.max", s.config.MemorySwapMax); err != nil {
		log.Printf("Warning: failed to set memory.swap.max=%s for job %s: %v", s.config.MemorySwapMax, job.ID, err)
	}
	if err := jobCGroup.SetValue("memory.max", s.config.MemoryMax); err != nil {
		log.Printf("Warning: failed to set memory.max=%s for job %s: %v", s.config.MemoryMax, job.ID, err)
	}
	if err := jobCGroup.SetValue("memory.oom.group", "1"); err != nil {
		log.Printf("Warning: failed to set memory.oom.group=1 for job %s: %v", job.ID, err)
		// Don't fail the job if we can't set this - it's not critical
	}

	jobName := fmt.Sprintf("job-%s", job.ID)

	// cleanupCtx is used for teardown (kill/delete) so it works even when ctx
	// has been canceled (e.g. the job was killed by dedup). If we used ctx, the
	// deferred kill/delete RPCs below would fail instantly with "context
	// canceled" without ever reaching containerd, leaving the container running
	// as a phantom that doesn't count against the concurrency limit.
	cleanupCtx := namespaces.WithNamespace(context.Background(), "bender")

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
			oci.WithCapabilities(nil),
			oci.WithNoNewPrivileges,
			withOOMScoreAdj(1000), // make jobs the preferred victims of the global OOM killer
			withSeccomp(s.config.SeccompLog),
		),
	)
	if err != nil {
		return err
	}
	defer container.Delete(cleanupCtx)

	log.Println("creating task")

	// create a new task
	task, err := container.NewTask(ctx, cio.NewCreator(
		cio.WithFIFODir(filepath.Join(s.config.DataDir, "fifo")),
		cio.WithStreams(nil, logs, logs),
	))
	if err != nil {
		return err
	}
	// Tear the task down on exit. We use cleanupCtx (not ctx) so this still runs
	// after the job is killed via dedup (which cancels ctx). We must kill, wait
	// for the task to actually exit, then delete: deleting a still-running task
	// fails. On the normal exit path the task is already dead, so the kill is a
	// no-op and the wait returns immediately.
	defer func() {
		exitC, err := task.Wait(cleanupCtx)
		if err != nil {
			log.Printf("cleanup: task.Wait failed for job %s: %v", job.ID, err)
		}
		if err := task.Kill(cleanupCtx, syscall.SIGKILL); err != nil {
			log.Printf("cleanup: task.Kill failed for job %s: %v", job.ID, err)
		}
		if exitC != nil {
			select {
			case <-exitC:
			case <-time.After(30 * time.Second):
				log.Printf("cleanup: timed out waiting for task to exit for job %s", job.ID)
			}
		}
		if _, err := task.Delete(cleanupCtx); err != nil {
			log.Printf("cleanup: task.Delete failed for job %s: %v", job.ID, err)
		}
	}()

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
		// Use "mv" instead of os.Rename because btrfs subvolumes are separate filesystems,
		// so os.Rename fails with "invalid cross-device link".
		err = doExec("mv", jobArtifactsDir, artifactsDir)
		if err != nil {
			log.Printf("failed to move artifact dir: %v", err)
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
