package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v88/github"
)

// This file implements the background duties that keep GitHub's merge queue
// from wedging when bender itself is the problem:
//
//  1. mergeQueuePollRun reconciles each configured repo's gh-readonly-queue
//     branches (GitHub's merge group branches) against bender's in-memory job
//     queue. Every queue branch means GitHub sent (or tried to send) a push
//     webhook; a branch with no live job and no recently finished one is a
//     notification bender missed — e.g. because it was restarting, which loses
//     the in-memory queue. After a grace period that accounts for GitHub's
//     event delivery delay, the job is reconstructed and put back into the
//     queue. Everything is computed in memory: nothing is written to disk.
//  2. staleJobStatusRun re-posts the pending GitHub status of jobs that have
//     been queued for a long time, and runJob (job.go) re-posts the status of
//     jobs that have been running for more than an hour. GitHub's merge queue
//     fails a group when a required check goes silent for too long (default 60
//     minutes), so long jobs must keep refreshing their status.
//
// The git refs API has no typed support for matching-refs in the shape we need
// here, so requests go through Client.NewRequest/Client.Do with the local
// gitRef type below.

// mergeQueueBranchRe matches the temporary branches GitHub creates for merge
// queue groups, capturing the target branch.
var mergeQueueBranchRe = regexp.MustCompile("^gh-readonly-queue/([^/]+)/")

// mergeQueuePRRe extracts the PR number from a merge queue branch name
// (gh-readonly-queue/<target>/pr-<number>-<sha>), when present.
var mergeQueuePRRe = regexp.MustCompile("^gh-readonly-queue/[^/]+/pr-([0-9]+)-")

// gitRef mirrors one entry of the "Get matching references" REST API response.
type gitRef struct {
	Ref    string `json:"ref"`
	Object struct {
		SHA string `json:"sha"`
	} `json:"object"`
}

type configuredRepo struct {
	owner    string
	name     string
	fullName string
}

// parseConfiguredRepos validates and dedups the merge_queue.repos config.
func parseConfiguredRepos(list []string) []configuredRepo {
	seen := map[string]bool{}
	var out []configuredRepo
	for _, entry := range list {
		owner, name, ok := strings.Cut(entry, "/")
		if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
			log.Printf("[mergequeue] ignoring malformed merge_queue.repos entry %q, want \"owner/repo\"", entry)
			continue
		}
		fullName := owner + "/" + name
		if seen[fullName] {
			continue
		}
		seen[fullName] = true
		out = append(out, configuredRepo{owner: owner, name: name, fullName: fullName})
	}
	return out
}

// mergeQueueTracker holds the poller's in-memory state. It is deliberately
// memory-only: all maps below are purged (per round, on write, or by TTL), so
// nothing grows without bound.
type mergeQueueTracker struct {
	mutex sync.Mutex

	// firstSeen records when a queue branch was first seen with no live bender
	// job, keyed by "<repo full name>/<sha>". An entry older than event_grace
	// is a missed notification and gets reconstructed. Pruned every poll round
	// against the live branch listing, and purged wholesale for repos that
	// leave the config.
	firstSeen map[string]time.Time

	// finished records when bender last completed a merge-queue job, keyed by
	// sha. It stops the poller from retriggering a job whose group is still in
	// the queue waiting on some *other* required check. Purged by job_ttl both
	// on write and on read.
	finished map[string]time.Time

	// installs caches repo full name -> installation ID, resolved once per
	// repo with app-level JWT auth. Bounded by len(configured repos).
	installs map[string]int64
}

func newMergeQueueTracker() *mergeQueueTracker {
	return &mergeQueueTracker{
		firstSeen: map[string]time.Time{},
		finished:  map[string]time.Time{},
		installs:  map[string]int64{},
	}
}

func (t *mergeQueueTracker) firstSeenGet(key string) time.Time {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.firstSeen[key]
}

func (t *mergeQueueTracker) firstSeenSet(key string, when time.Time) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.firstSeen[key] = when
}

func (t *mergeQueueTracker) firstSeenClear(key string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	delete(t.firstSeen, key)
}

// firstSeenPrune drops timestamps for branches that left the queue.
func (t *mergeQueueTracker) firstSeenPrune(repoFullName string, keep map[string]bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	prefix := repoFullName + "/"
	for k := range t.firstSeen {
		if strings.HasPrefix(k, prefix) && !keep[k] {
			delete(t.firstSeen, k)
		}
	}
}

// firstSeenPurgeUnknown drops timestamps for repos that are no longer in the
// config, so shrinking the config releases memory.
func (t *mergeQueueTracker) firstSeenPurgeUnknown(valid map[string]bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for k := range t.firstSeen {
		fullName, _, _ := strings.Cut(k, "/")
		if !valid[fullName] {
			delete(t.firstSeen, k)
		}
	}
}

// finishedRecently reports whether bender completed a merge-queue job for this
// sha within ttl. This prevents retriggering a job whose group is still in the
// queue waiting on another required check.
func (t *mergeQueueTracker) finishedRecently(sha string, ttl time.Duration) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	when, ok := t.finished[sha]
	return ok && ttl > 0 && time.Since(when) < ttl
}

// recordFinished remembers a completed merge-queue job's sha, purging expired
// entries on write so the map stays bounded by job throughput x ttl.
func (t *mergeQueueTracker) recordFinished(sha string, ttl time.Duration) {
	if sha == "" || ttl <= 0 {
		return
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for s, when := range t.finished {
		if time.Since(when) >= ttl {
			delete(t.finished, s)
		}
	}
	t.finished[sha] = time.Now()
}

// installFor returns the installation ID bender uses for a repo, resolving it
// once with app-level JWT auth and caching the result in memory.
func (s *Service) installFor(ctx context.Context, fullName, owner, name string) (int64, error) {
	s.mq.mutex.Lock()
	if id, ok := s.mq.installs[fullName]; ok {
		s.mq.mutex.Unlock()
		return id, nil
	}
	s.mq.mutex.Unlock()

	atr, err := ghinstallation.NewAppsTransport(http.DefaultTransport, s.config.Github.AppID, []byte(s.config.Github.PrivateKey))
	if err != nil {
		return 0, err
	}
	gh, err := github.NewClient(github.WithHTTPClient(&http.Client{Transport: atr}))
	if err != nil {
		return 0, err
	}
	inst, _, err := gh.Apps.GetRepositoryInstallation(ctx, owner, name)
	if err != nil {
		return 0, err
	}
	s.mq.mutex.Lock()
	s.mq.installs[fullName] = inst.GetID()
	s.mq.mutex.Unlock()
	return inst.GetID(), nil
}

// pollInterval returns the configured poll interval, with a default.
func (s *Service) pollInterval() time.Duration {
	if ivl := s.config.MergeQueue.PollInterval; ivl > 0 {
		return ivl
	}
	return 5 * time.Minute
}

func (s *Service) mergeQueuePollRun() {
	interval := s.pollInterval()
	log.Printf("[mergequeue] merge queue poller started, polling every %s", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.pollMergeQueues()
		<-ticker.C
	}
}

// staleJobStatusRun periodically re-posts the GitHub status of jobs that have
// been waiting in the in-memory queue for longer than status_refresh_interval.
func (s *Service) staleJobStatusRun() {
	interval := s.pollInterval()
	log.Printf("[mergequeue] stale queued-job status refresher started, running every %s", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.refreshStaleQueuedJobs()
		<-ticker.C
	}
}

func (s *Service) pollMergeQueues() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repos := parseConfiguredRepos(s.config.MergeQueue.Repos)
	valid := map[string]bool{}
	for _, r := range repos {
		valid[r.fullName] = true
	}
	s.mq.firstSeenPurgeUnknown(valid)

	for _, r := range repos {
		instID, err := s.installFor(ctx, r.fullName, r.owner, r.name)
		if err != nil {
			log.Printf("[mergequeue] %s: resolving installation: %v", r.fullName, err)
			continue
		}
		gh, err := s.githubClient(instID)
		if err != nil {
			log.Printf("[mergequeue] github client for installation %d: %v", instID, err)
			continue
		}
		s.pollRepoQueueBranches(ctx, gh, instID, r)
	}
}

// pollRepoQueueBranches reconciles the repo's gh-readonly-queue branches
// against bender's in-memory job queue, and reconstructs notifications that
// were missed.
func (s *Service) pollRepoQueueBranches(ctx context.Context, gh *github.Client, instID int64, r configuredRepo) {
	refs, err := s.listQueueBranches(ctx, gh, r)
	if err != nil {
		// A gone or renamed repo 404s; anything else is worth logging.
		if !is404(err) {
			log.Printf("[mergequeue] %s: listing queue branches: %v", r.fullName, err)
		}
		return
	}

	grace := s.config.MergeQueue.EventGrace
	if grace <= 0 {
		grace = 15 * time.Minute
	}

	keep := map[string]bool{}
	for _, ref := range refs {
		const prefix = "refs/heads/"
		if !strings.HasPrefix(ref.Ref, prefix) {
			continue
		}
		branch := ref.Ref[len(prefix):]
		sha := ref.Object.SHA
		if sha == "" {
			continue
		}
		key := r.fullName + "/" + sha
		keep[key] = true

		if s.queue.hasLiveJobFor(r.fullName, sha) || s.mq.finishedRecently(sha, s.config.MergeQueue.JobTTL) {
			s.mq.firstSeenClear(key)
			continue
		}

		first := s.mq.firstSeenGet(key)
		if first.IsZero() {
			s.mq.firstSeenSet(key, time.Now())
			log.Printf("[mergequeue] %s: queue branch %s (sha %s) has no live bender job, waiting %s for the webhook", r.fullName, branch, sha, grace)
			continue
		}
		if time.Since(first) < grace {
			continue
		}

		s.handleStuckEntry(ctx, gh, instID, r, sha, branch)
		// Don't handle the same branch again until event_grace elapses once more.
		s.mq.firstSeenSet(key, time.Now())
	}
	s.mq.firstSeenPrune(r.fullName, keep)
}

// listQueueBranches returns the repo's gh-readonly-queue branches, capped so a
// pathological repo can't make the poller run away.
func (s *Service) listQueueBranches(ctx context.Context, gh *github.Client, r configuredRepo) ([]gitRef, error) {
	var out []gitRef
	page := 1
	for {
		url := fmt.Sprintf("repos/%s/%s/git/matching-refs/heads/gh-readonly-queue?per_page=100&page=%d", r.owner, r.name, page)
		req, err := gh.NewRequest(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}
		var batch []gitRef
		resp, err := gh.Do(req, &batch)
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
		if resp.NextPage == 0 {
			return out, nil
		}
		page = resp.NextPage
		if len(out) >= 1000 {
			log.Printf("[mergequeue] %s: over 1000 queue branches, ignoring the rest", r.fullName)
			return out, nil
		}
	}
}

// handleStuckEntry puts a merge queue branch whose notification was missed
// back into the in-memory queue. The default action: reconstruct the push
// event and enqueue it like a fresh webhook.
func (s *Service) handleStuckEntry(ctx context.Context, gh *github.Client, instID int64, r configuredRepo, sha, branch string) {
	action := s.config.MergeQueue.Action
	if action == "" {
		action = "retrigger"
	}
	log.Printf("[mergequeue] %s: queue branch %s (sha %s) is STUCK: missed its push notification, action=%s", r.fullName, branch, sha, action)

	switch action {
	case "retrigger":
		s.retriggerQueueBranch(ctx, gh, instID, r, sha, branch)
	case "fail":
		s.failQueueBranch(ctx, gh, r, sha)
	default:
		log.Printf("[mergequeue] unknown merge_queue.action %q, set it to \"retrigger\" or \"fail\"", action)
	}
}

func (s *Service) failQueueBranch(ctx context.Context, gh *github.Client, r configuredRepo, sha string) {
	contexts := s.config.MergeQueue.FailContexts
	if len(contexts) == 0 {
		log.Printf("[mergequeue] action \"fail\" needs merge_queue.fail_contexts to be set, doing nothing")
		return
	}
	for _, context := range contexts {
		_, _, err := gh.Repositories.CreateStatus(ctx, r.owner, r.name, sha, github.RepoStatus{
			State:       github.Ptr("failure"),
			Context:     github.Ptr(context),
			Description: github.Ptr("bender: no job running for this merge queue entry"),
		})
		if err != nil {
			log.Printf("[mergequeue] %s: posting failure status %q on %s: %v", r.fullName, context, sha, err)
		}
	}
}

// retriggerQueueBranch puts a missed merge queue job back into the in-memory
// queue: it rebuilds the push event the queue branch originally arrived as and
// runs it through the normal event -> jobs -> queue pipeline. The branch is
// known exactly (it still exists), so no reconstruction is needed.
func (s *Service) retriggerQueueBranch(ctx context.Context, gh *github.Client, instID int64, r configuredRepo, sha, branch string) {
	target := ""
	if m := mergeQueueBranchRe.FindStringSubmatch(branch); m != nil {
		target = m[1]
	}

	// The PR number is encoded in the branch name; recover it for the
	// dashboard link and the per-PR cache.
	var pr *github.PullRequest
	cache := []string{}
	if m := mergeQueuePRRe.FindStringSubmatch(branch); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil {
			pr = &github.PullRequest{
				Number:  github.Ptr(n),
				HTMLURL: github.Ptr(fmt.Sprintf("https://github.com/%s/pull/%d", r.fullName, n)),
			}
			cache = append(cache, fmt.Sprintf("pr-%d", n))
		}
	}
	if target != "" {
		cache = append(cache, fmt.Sprintf("branch-%s", target))
	}

	event := &Event{
		Event: "push",
		Attributes: map[string]string{
			"branch": branch,
		},
		Repo: &github.Repository{
			Name:     github.Ptr(r.name),
			FullName: github.Ptr(r.fullName),
			Owner:    &github.User{Login: github.Ptr(r.owner)},
			CloneURL: github.Ptr(fmt.Sprintf("https://github.com/%s.git", r.fullName)),
			HTMLURL:  github.Ptr(fmt.Sprintf("https://github.com/%s", r.fullName)),
		},
		PullRequest:    pr,
		CloneURL:       fmt.Sprintf("https://github.com/%s.git", r.fullName),
		SHA:            sha,
		InstallationID: instID,
		Cache:          cache,
		Trusted:        true,
	}

	jobs, err := s.handleEvent(ctx, gh, event)
	if err != nil {
		log.Printf("[mergequeue] %s: retriggering sha %s: %v", r.fullName, sha, err)
		return
	}
	if len(jobs) == 0 {
		log.Printf("[mergequeue] %s: sha %s produced no jobs (no .github/ci job matches push events at this sha)", r.fullName, sha)
		return
	}
	log.Printf("[mergequeue] %s: putting %d missed job(s) back into the queue for %s", r.fullName, len(jobs), branch)
	s.queue.enqueueJobs(jobs, s)
}

// refreshStaleQueuedJobs re-posts the pending status of jobs that have been
// queued for longer than status_refresh_interval, so GitHub (and in particular
// the merge queue) doesn't consider them abandoned.
func (s *Service) refreshStaleQueuedJobs() {
	interval := s.config.StatusRefreshInterval
	if interval <= 0 {
		return
	}
	for _, job := range s.queue.getStaleQueuedJobs(interval) {
		gh, err := s.githubClient(job.InstallationID)
		if err != nil {
			log.Printf("error creating github client for queued status refresh: %v", err)
			continue
		}
		desc := fmt.Sprintf("Job enqueued, waiting %s", formatDuration(time.Since(job.EnqueuedAt)))
		if err := s.setStatus(context.Background(), gh, job, "pending", desc); err != nil {
			log.Printf("error refreshing queued status for job %s: %v", job.ID, err)
		}
	}
}
