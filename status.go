package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/go-github/v88/github"
	"github.com/sqlbunny/errors"
)

// Variables rather than constants so tests can shrink them.
var (
	statusRetryInitialDelay = time.Second
	statusRetryMaxDelay     = 5 * time.Minute
	// Give up on an update after this long. GitHub outages rarely last longer,
	// and a status that shows up hours late is more confusing than useful.
	statusRetryGiveUp = 2 * time.Hour
)

// statusKey identifies one commit status on GitHub. GitHub only keeps the
// latest state per key, so that's the granularity updates are ordered at.
type statusKey struct {
	owner, repo, sha, context string
}

type statusUpdate struct {
	gh     *github.Client
	jobID  string
	status github.RepoStatus
}

type statusSlot struct {
	// next is the newest update not yet sent, if any. A newer update replaces
	// it: there's no point sending a state GitHub would immediately forget.
	next *statusUpdate
	// wake interrupts the worker's retry sleep when next changes.
	wake chan struct{}
}

// statusSender posts commit statuses to GitHub in the background, retrying
// transient failures with exponential backoff.
//
// There's at most one worker per statusKey, which sends updates strictly one
// after another, so an older update can never land after a newer one: if an
// earlier "pending" is still being retried when the final "success" is set,
// the worker drops the "pending" and sends the "success" instead.
//
// The zero value is ready to use.
type statusSender struct {
	mu    sync.Mutex
	slots map[statusKey]*statusSlot
}

func (ss *statusSender) set(key statusKey, u *statusUpdate) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.slots == nil {
		ss.slots = make(map[statusKey]*statusSlot)
	}
	slot, ok := ss.slots[key]
	if ok {
		// A worker is already running for this key; hand it the new update.
		slot.next = u
		select {
		case slot.wake <- struct{}{}:
		default:
		}
		return
	}
	slot = &statusSlot{next: u, wake: make(chan struct{}, 1)}
	ss.slots[key] = slot
	go ss.worker(key, slot)
}

// take returns the next update to send, or nil (removing the slot, so the
// worker exits) if there is none.
func (ss *statusSender) take(key statusKey, slot *statusSlot) *statusUpdate {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	u := slot.next
	slot.next = nil
	if u == nil {
		delete(ss.slots, key)
	}
	return u
}

// superseded reports whether a newer update is waiting.
func (ss *statusSender) superseded(slot *statusSlot) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return slot.next != nil
}

func (ss *statusSender) worker(key statusKey, slot *statusSlot) {
	for {
		u := ss.take(key, slot)
		if u == nil {
			return
		}
		ss.send(key, slot, u)
	}
}

// send posts u, retrying until it succeeds, fails permanently, times out, or
// a newer update for the same key supersedes it.
func (ss *statusSender) send(key statusKey, slot *statusSlot, u *statusUpdate) {
	start := time.Now()
	delay := statusRetryInitialDelay
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _, err := u.gh.Repositories.CreateStatus(ctx, key.owner, key.repo, key.sha, u.status)
		cancel()
		if err == nil {
			return
		}
		if !isRetryableGithubError(err) {
			log.Printf("error setting %s status for job %s, not retrying: %v", u.status.GetState(), u.jobID, err)
			return
		}
		if time.Since(start)+delay > statusRetryGiveUp {
			log.Printf("error setting %s status for job %s, giving up after %d attempts: %v", u.status.GetState(), u.jobID, attempt, err)
			return
		}
		log.Printf("error setting %s status for job %s (attempt %d), retrying in %s: %v", u.status.GetState(), u.jobID, attempt, delay, err)

		select {
		case <-slot.wake:
		case <-time.After(delay):
		}
		if ss.superseded(slot) {
			log.Printf("dropping %s status for job %s: superseded by a newer one", u.status.GetState(), u.jobID)
			return
		}
		delay = min(delay*2, statusRetryMaxDelay)
	}
}

// isRetryableGithubError reports whether a GitHub API error is likely
// transient. Server errors (5xx), 429 and network errors are retried; other
// 4xx responses mean the request itself is bad and retrying won't help.
func isRetryableGithubError(err error) bool {
	var errResp *github.ErrorResponse
	if errors.As(err, &errResp) && errResp.Response != nil {
		code := errResp.Response.StatusCode
		return code >= 500 || code == http.StatusTooManyRequests
	}
	return true
}

// setStatus queues a commit status update for the job. It returns
// immediately; the update is sent (and retried) in the background.
func (s *Service) setStatus(gh *github.Client, j *Job, state string, description string) {
	url := fmt.Sprintf("%s/jobs/%s", s.config.ExternalURL, j.ID)
	ctxName := fmt.Sprintf("ci/%s", j.Name)
	key := statusKey{
		owner:   *j.Repo.Owner.Login,
		repo:    *j.Repo.Name,
		sha:     j.SHA,
		context: ctxName,
	}
	s.statuses.set(key, &statusUpdate{
		gh:    gh,
		jobID: j.ID,
		status: github.RepoStatus{
			State:       github.Ptr(state),
			Context:     github.Ptr(ctxName),
			Description: github.Ptr(description),
			TargetURL:   &url,
		},
	})
}
