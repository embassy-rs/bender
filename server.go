package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/go-github/v72/github"
	"github.com/sqlbunny/errors"
)

func (s *Service) serverRun() {

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/jobs/{jobID}", s.HandleJobLogs)
	r.Get("/jobs/{jobID}/artifacts", http.RedirectHandler("artifacts/", http.StatusMovedPermanently).ServeHTTP)
	r.Get("/jobs/{jobID}/artifacts/*", s.HandleJobArtifacts)
	r.Post("/webhook", func(w http.ResponseWriter, r *http.Request) {
		err := s.handleWebhook(r)
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
		} else {
			w.WriteHeader(200)
		}
	})

	log.Println("server started")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.config.ListenPort), r))
}

func validJobID(id string) bool {
	ok, err := regexp.MatchString("^[a-z0-9]+$", id)
	return err == nil && ok
}

func (s *Service) HandleJobArtifacts(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")
	if !validJobID(jobID) {
		log.Printf("invalid job ID: '%s'", jobID)
		http.Error(w, http.StatusText(404), 404)
		return
	}

	// serve files from data/artifacts/<jobID>/
	http.StripPrefix("/jobs/"+jobID+"/artifacts/", http.FileServer(http.Dir(filepath.Join(s.config.DataDir, "artifacts", jobID)))).ServeHTTP(w, r)
}

func (s *Service) HandleJobLogs(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")
	if !validJobID(jobID) {
		log.Printf("invalid job ID: '%s'", jobID)
		http.Error(w, http.StatusText(404), 404)
		return
	}

	f, err := os.Open(filepath.Join(s.config.DataDir, "logs", jobID))
	if err != nil {
		log.Printf("failed to open log file: %v", err)
		http.Error(w, http.StatusText(404), 404)
		return
	}

	w.Header().Add("Content-Type", "text/html; charset=utf-8")
	w.Header().Add("X-Content-Type-Options", "nosniff")

	buf := make([]byte, 32*1024)

	if s.isJobRunning(jobID) {
		// padding to make browsers instantly start rendering the document
		// as it arrives from the network. Browsers seem to wait until a minimum
		// of data has been received before rendering anything...
		for i := range buf {
			buf[i] = ' '
		}
		w.Write(buf)
	}

	io.WriteString(w, `
	<!DOCTYPE html>
	<html>
		<head>
			<title>lol job</title>
			<style type="text/css">
				#main {
					overflow-anchor: none;
					font-family: monospace;
					white-space: pre;
				}
				body::after {
					overflow-anchor: auto;
					content: "   ";
					display: block;
					height: 1px;
				}
			</style>
		</head>
		<body>
			<div id="main">`)
	for {
		n, err := f.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			log.Printf("failed to read logs: %v", err)
			http.Error(w, http.StatusText(500), 500)
			return
		}

		if n == 0 {
			if !s.isJobRunning(jobID) {
				return
			}

			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		escaped := html.EscapeString(string(buf[:n]))
		_, err = io.WriteString(w, escaped)
		if err != nil {
			log.Printf("failed to send logs: %v", err)
			http.Error(w, http.StatusText(500), 500)
			return
		}
	}
}

func (s *Service) handleWebhook(r *http.Request) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	payload, err := github.ValidatePayload(r, []byte(s.config.Github.WebhookSecret))
	defer r.Body.Close()
	if err != nil {
		log.Printf("error validating request body: err=%s\n", err)
		return nil
	}

	installationID, err := parseEventInstallationID(payload)
	if err != nil {
		log.Printf("could not get installation id from webhook: err=%s\n", err)
		return nil
	}
	gh, err := s.githubClient(installationID)
	if err != nil {
		return err
	}

	ee, err := github.ParseWebHook(github.WebHookType(r), payload)
	if err != nil {
		log.Printf("could not parse webhook: err=%s\n", err)
		return nil
	}

	var events []*Event
	switch e := ee.(type) {
	case *github.PushEvent:
		branch, ok := strings.CutPrefix(*e.Ref, "refs/heads/")
		if !ok {
			log.Printf("unknown ref '%s'", *e.Ref)
			return nil
		}

		cacheBranch := branch
		if m := regexp.MustCompile("^gh-readonly-queue/([^/]+)/").FindStringSubmatch(branch); m != nil {
			cacheBranch = m[1]
			log.Printf("branch '%s' is from merge queue, using target branch '%s' for cache", branch, cacheBranch)
		}

		if e.HeadCommit == nil {
			// this is a branch deletion.
			return nil
		}

		events = append(events, &Event{
			Event: "push",
			Attributes: map[string]string{
				"branch": branch,
			},
			Repo:           getRepoFromPushEvent(e),
			SHA:            *e.HeadCommit.ID,
			InstallationID: *e.Installation.ID,
			Cache: []string{
				fmt.Sprintf("branch-%s", cacheBranch),
				fmt.Sprintf("branch-%s", *e.Repo.DefaultBranch),
			},
			Trusted: true,
		})
	case *github.PullRequestEvent:
		if *e.Action == "opened" || *e.Action == "synchronize" {
			events = append(events, &Event{
				Event: "pull_request",
				Attributes: map[string]string{
					"branch": *e.PullRequest.Base.Ref,
				},
				Repo:           e.Repo,
				PullRequest:    e.PullRequest,
				CloneURL:       *e.PullRequest.Head.Repo.CloneURL,
				SHA:            *e.PullRequest.Head.SHA,
				InstallationID: *e.Installation.ID,
				Cache: []string{
					fmt.Sprintf("pr-%d", *e.PullRequest.Number),
					fmt.Sprintf("branch-%s", *e.PullRequest.Base.Ref),
					fmt.Sprintf("branch-%s", *e.Repo.DefaultBranch),
				},
				Trusted: isPRTrusted(e.Repo, e.PullRequest),
			})
		}
	case *github.IssueCommentEvent:
		if *e.Action == "created" {
			err := s.handleCommands(ctx, gh, &events, e)
			if err != nil {
				log.Printf("failed handling commands: %v", err)
			}
		}
	}

	if len(events) == 0 {
		return nil
	}

	for _, event := range events {
		if event.CloneURL == "" {
			event.CloneURL = *event.Repo.CloneURL
		}
		if event.Attributes == nil {
			event.Attributes = map[string]string{}
		}

		err = s.handleEvent(ctx, gh, event)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) handleCommands(ctx context.Context, gh *github.Client, outEvents *[]*Event, e *github.IssueCommentEvent) error {
	errors := ""

	for _, line := range strings.Split(*e.Comment.Body, "\n") {
		command, ok := strings.CutPrefix(line, "bender ")
		if !ok {
			continue
		}

		err := s.handleCommand(ctx, gh, outEvents, e, command)
		if err != nil {
			log.Printf("Failed to handle command `%s`: %v", command, err)
			errors += fmt.Sprintf("`%s`: %v\n", command, err)
		}
	}

	if errors != "" {
		_, _, err := gh.Issues.CreateComment(ctx, *e.Repo.Owner.Login, *e.Repo.Name, *e.Issue.Number, &github.IssueComment{
			Body: github.Ptr(errors),
		})
		if err != nil {
			log.Printf("Failed to post comment with command errors: %v", err)
		}
	}

	return nil
}

func (s *Service) handleCommand(ctx context.Context, gh *github.Client, outEvents *[]*Event, e *github.IssueCommentEvent, command string) error {
	dir, err := parseDirective(command)
	if err != nil {
		return err
	}

	if len(dir.Args) == 0 {
		return errors.New("no command?")
	}

	switch dir.Args[0] {
	case "run":
		if len(dir.Args) != 1 || len(dir.Conditions) != 0 {
			return errors.Errorf("'run' takes no arguments")
		}

		// check perms
		perms, _, err := gh.Repositories.GetPermissionLevel(ctx, *e.Repo.Owner.Login, *e.Repo.Name, *e.Comment.User.Login)
		if err != nil {
			return err
		}
		if *perms.Permission != "admin" && *perms.Permission != "write" {
			return errors.Errorf("permission denied")
		}

		// get PR
		if e.Issue.PullRequestLinks == nil {
			return errors.Errorf("This is not a pull request!")
		}
		pr, _, err := gh.PullRequests.Get(ctx, *e.Repo.Owner.Login, *e.Repo.Name, *e.Issue.Number)
		if err != nil {
			return err
		}

		*outEvents = append(*outEvents, &Event{
			Event: "pull_request",
			Attributes: map[string]string{
				"branch": *pr.Base.Ref,
			},
			Repo:           e.Repo,
			PullRequest:    pr,
			CloneURL:       *pr.Head.Repo.CloneURL,
			SHA:            *pr.Head.SHA,
			InstallationID: *e.Installation.ID,
			Cache: []string{
				fmt.Sprintf("pr-%d", *pr.Number),
				fmt.Sprintf("branch-%s", *pr.Base.Ref),
				fmt.Sprintf("branch-%s", *e.Repo.DefaultBranch),
			},
			Trusted: isPRTrusted(e.Repo, pr),
		})
		return nil
	default:
		return errors.Errorf("unknown command '%s'", dir.Args[0])
	}
}

func (s *Service) handleEvent(ctx context.Context, gh *github.Client, event *Event) error {
	getOpts := &github.RepositoryContentGetOptions{
		Ref: event.SHA,
	}
	_, dir, _, err := gh.Repositories.GetContents(ctx, *event.Repo.Owner.Login, *event.Repo.Name, ".github/ci", getOpts)
	if is404(err) {
		log.Printf("`.github/ci` directory does not exist")
		return nil
	} else if err != nil {
		return err
	} else if dir == nil {
		log.Printf("`.github/ci` is not a directory")
		return nil
	}

	var jobs []*Job

	for _, f := range dir {
		if *f.Type != "file" {
			continue
		}

		file, _, _, err := gh.Repositories.GetContents(ctx, *event.Repo.Owner.Login, *event.Repo.Name, *f.Path, getOpts)
		if err != nil {
			return err
		}

		content, err := file.GetContent()
		if err != nil {
			return err
		}

		meta, err := parseMeta(content)
		if err != nil {
			log.Printf("failed to parse meta for file '%s': %v", *f.Name, err)
			continue
		}

		matched := false

		for _, me := range meta.Events {
			if me.Event != event.Event {
				continue
			}

			ok := true
			for _, condition := range me.Conditions {
				if !condition.matches(event.Attributes) {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}

			matched = true
			break
		}

		if matched {
			jobs = append(jobs, &Job{
				ID:              makeJobID(),
				Event:           event,
				Name:            removeExtension(*f.Name),
				Script:          *f.Path,
				Permissions:     meta.Permissions,
				PermissionRepos: meta.PermissionRepos,
			})
		}
	}

	for _, job := range jobs {
		go s.runJob(context.Background(), job)
	}

	return nil
}

func parseEventInstallationID(payload []byte) (int64, error) {
	type Installation struct {
		ID *int64 `json:"id"`
	}
	type Event struct {
		Installation Installation `json:"installation"`
	}

	var e Event
	if err := json.Unmarshal(payload, &e); err != nil {
		return 0, err
	}

	if e.Installation.ID == nil {
		return 0, errors.New("no installation id in event")
	}

	return *e.Installation.ID, nil
}

func isPRTrusted(repo *github.Repository, pr *github.PullRequest) bool {
	// Trusted if the PR is not from a fork.
	if *pr.Head.Repo.Owner.Login == *repo.Owner.Login {
		return true
	}

	// Trusted if the PR has a "trusted" label
	for _, l := range pr.Labels {
		if l.Name != nil && *l.Name == "trusted" {
			return true
		}
	}

	return false
}
