package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
	r.Get("/", s.HandleDashboard)
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

// DashboardData holds the data for the dashboard template
type DashboardData struct {
	AllJobs     []*JobDisplayInfo
	LastUpdated string
}

// JobDisplayInfo holds the display information for a job
type JobDisplayInfo struct {
	ID            string
	Name          string        // Job name
	Repository    template.HTML // Changed to template.HTML for clickable links
	SHA           template.HTML // Changed to template.HTML for clickable links
	Priority      int
	DedupMode     string
	GitHubPRLink  template.HTML
	LogsURL       string
	Status        string // Running, Queued, Waiting
	QueuePosition int    // 0 for non-queued jobs
}

var dashboardTemplate = template.Must(template.New("dashboard").Funcs(template.FuncMap{
	"add": func(a, b int) int { return a + b },
}).Parse(`<!DOCTYPE html>
<html>
<head>
    <title>Bender CI Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .status-running { background-color: #d4edda; }
        .status-queued { background-color: #fff3cd; }
        .status-waiting { background-color: #d1ecf1; }
        a { color: #007bff; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .queue-pos { text-align: center; }
        .queue-pos.empty { color: #ccc; }
    </style>
</head>
<body>
    <h1>Bender CI Dashboard</h1>
    
    <table>
        <tr>
            <th>Status</th>
            <th>#</th>
            <th>Job ID</th>
            <th>Job Name</th>
            <th>Repository</th>
            <th>SHA</th>
            <th>Prio</th>
            <th>Dedup</th>
            <th>GitHub PR</th>
            <th>Logs</th>
        </tr>
        {{range .AllJobs}}
        <tr class="status-{{.Status}}">
            <td>{{.Status}}</td>
            <td class="queue-pos{{if eq .QueuePosition 0}} empty{{end}}">
                {{if gt .QueuePosition 0}}{{.QueuePosition}}{{else}}-{{end}}
            </td>
            <td>{{.ID}}</td>
            <td>{{.Name}}</td>
            <td>{{.Repository}}</td>
            <td>{{.SHA}}</td>
            <td>{{.Priority}}</td>
            <td>{{.DedupMode}}</td>
            <td>{{.GitHubPRLink}}</td>
            <td><a href="{{.LogsURL}}">View Logs</a></td>
        </tr>
        {{end}}
    </table>
    
    <p><small>Last updated: {{.LastUpdated}}</small></p>
</body>
</html>`))

func (s *Service) HandleDashboard(w http.ResponseWriter, r *http.Request) {
	s.runningJobsMutex.Lock()
	defer s.runningJobsMutex.Unlock()

	var allJobs []*JobDisplayInfo

	// Collect running jobs and sort them by when they started (longest running first)
	var runningJobsSlice []*Job
	for _, job := range s.runningJobs {
		runningJobsSlice = append(runningJobsSlice, job)
	}
	
	// Sort by start time - earliest start time first (longest running)
	sort.Slice(runningJobsSlice, func(i, j int) bool {
		return runningJobsSlice[i].StartTime.Before(runningJobsSlice[j].StartTime)
	})

	for _, job := range runningJobsSlice {
		info := createJobDisplayInfo(job)
		info.Status = "running"
		info.QueuePosition = 0
		allJobs = append(allJobs, info)
	}

	// Collect pending jobs (already sorted by priority in the queue)
	pendingJobs := s.jobQueue.GetAllJobs()
	for i, job := range pendingJobs {
		info := createJobDisplayInfo(job)
		info.Status = "queued"
		info.QueuePosition = i + 1
		allJobs = append(allJobs, info)
	}

	// Collect waiting jobs and sort them by ID for consistent ordering
	var waitingJobsSlice []*Job
	for _, jobs := range s.waitingJobs {
		for _, job := range jobs {
			waitingJobsSlice = append(waitingJobsSlice, job)
		}
	}
	sort.Slice(waitingJobsSlice, func(i, j int) bool {
		return waitingJobsSlice[i].ID < waitingJobsSlice[j].ID
	})

	for _, job := range waitingJobsSlice {
		info := createJobDisplayInfo(job)
		info.Status = "waiting"
		info.QueuePosition = 0
		allJobs = append(allJobs, info)
	}

	data := DashboardData{
		AllJobs:     allJobs,
		LastUpdated: time.Now().Format("2006-01-02 15:04:05"),
	}

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	err := dashboardTemplate.Execute(w, data)
	if err != nil {
		log.Printf("Error executing dashboard template: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func createJobDisplayInfo(job *Job) *JobDisplayInfo {
	info := &JobDisplayInfo{
		ID:        job.ID,
		Name:      job.Name,
		Priority:  job.Priority,
		DedupMode: job.Dedup.String(),
		LogsURL:   "/jobs/" + job.ID,
	}

	// Safe repository name as clickable link
	if job.Repo != nil && job.Repo.FullName != nil && job.Repo.HTMLURL != nil {
		info.Repository = template.HTML(fmt.Sprintf(`<a href="%s" target="_blank">%s</a>`,
			template.HTMLEscapeString(*job.Repo.HTMLURL),
			template.HTMLEscapeString(*job.Repo.FullName)))
	} else if job.Repo != nil && job.Repo.FullName != nil {
		info.Repository = template.HTML(template.HTMLEscapeString(*job.Repo.FullName))
	} else {
		info.Repository = template.HTML("unknown")
	}

	// Safe SHA as clickable link to commit
	shortSHA := job.SHA[:8]
	if job.Repo != nil && job.Repo.HTMLURL != nil {
		commitURL := fmt.Sprintf("%s/commit/%s", *job.Repo.HTMLURL, job.SHA)
		info.SHA = template.HTML(fmt.Sprintf(`<a href="%s" target="_blank">%s</a>`,
			template.HTMLEscapeString(commitURL),
			template.HTMLEscapeString(shortSHA)))
	} else {
		info.SHA = template.HTML(template.HTMLEscapeString(shortSHA))
	}

	// Safe GitHub PR link generation
	if job.PullRequest != nil && job.PullRequest.Number != nil && job.PullRequest.HTMLURL != nil {
		// Use template.HTML to mark this as safe HTML since we're generating it safely
		info.GitHubPRLink = template.HTML(fmt.Sprintf(`<a href="%s" target="_blank">PR #%d</a>`,
			template.HTMLEscapeString(*job.PullRequest.HTMLURL),
			*job.PullRequest.Number))
	} else {
		info.GitHubPRLink = template.HTML("-")
	}

	return info
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
			// Determine dedup mode: default to kill for PR jobs, otherwise use metadata
			dedupMode := meta.Dedup
			if dedupMode == DedupNone && event.Event == "pull_request" {
				dedupMode = DedupKill
			}

			jobs = append(jobs, &Job{
				ID:              makeJobID(),
				Event:           event,
				Name:            removeExtension(*f.Name),
				Priority:        meta.Priority,
				Dedup:           dedupMode,
				Script:          *f.Path,
				Permissions:     meta.Permissions,
				PermissionRepos: meta.PermissionRepos,
			})
		}
	}

	for _, job := range jobs {
		s.enqueueJob(job)
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
