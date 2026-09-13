package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/go-github/v88/github"
	"github.com/sqlbunny/errors"
)

const (
	// How long we remember the GUID of a delivery we've handled, and therefore
	// how far back the poller is willing to look. Anything older than this we
	// can no longer tell apart from a delivery we already processed, so we
	// leave it alone.
	deliveryMemory = 6 * time.Hour

	// How often to reconcile against GitHub's delivery list.
	deliveryPollInterval = 5 * time.Minute

	// Cap on pages walked per poll, so a burst of traffic can't turn one tick
	// into thousands of API calls. 100 per page, newest first.
	deliveryPollMaxPages = 10
)

// seenDeliveries remembers the GUIDs of webhook deliveries we've handled, so
// the poller can tell a delivery that got lost on the way to us from one that
// arrived normally.
//
// It's deliberately RAM-only: a restart loses the memory, but the poller also
// refuses to look back past its own boot time, so there's nothing to remember
// about the time before we existed.
type seenDeliveries struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newSeenDeliveries() *seenDeliveries {
	return &seenDeliveries{seen: map[string]time.Time{}}
}

// add records a delivery GUID as handled.
func (d *seenDeliveries) add(guid string) {
	if guid == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[guid] = time.Now()
}

// has reports whether we've already handled this delivery GUID.
func (d *seenDeliveries) has(guid string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[guid]
	return ok
}

// prune drops entries the poller would no longer act on anyway.
func (d *seenDeliveries) prune(now time.Time) {
	cutoff := now.Add(-deliveryMemory)
	d.mu.Lock()
	defer d.mu.Unlock()
	for guid, t := range d.seen {
		if t.Before(cutoff) {
			delete(d.seen, guid)
		}
	}
}

// deliveryPollRun periodically reconciles what GitHub thinks it sent us against
// what we actually received, and processes anything that went missing.
//
// GitHub does not retry failed webhook deliveries, and deliveries occasionally
// vanish between GitHub and us, so a webhook lost in transit means a CI run
// that simply never happens. This is the backstop for that.
func (s *Service) deliveryPollRun() {
	for {
		// Sleep first: nothing can qualify for a rescue until we've been up
		// long enough for a delivery to have arrived and been missed.
		time.Sleep(deliveryPollInterval)

		if err := s.pollDeliveries(); err != nil {
			log.Printf("[poll] failed to poll webhook deliveries: %v", err)
		}
	}
}

func (s *Service) pollDeliveries() error {
	ctx, cancel := context.WithTimeout(context.Background(), deliveryPollInterval)
	defer cancel()

	// The deliveries endpoints are app-level: they take a JWT signed with the
	// app key, and reject installation tokens.
	gh, err := s.githubAppClient()
	if err != nil {
		return err
	}

	now := time.Now()
	s.deliveries.prune(now)

	// Only rescue deliveries newer than both our GUID memory (older ones we
	// can't distinguish from already-handled) and our boot time (deliveries
	// from before we started were some other process's to handle, and
	// re-running CI for events that stale is worse than dropping them).
	cutoff := now.Add(-deliveryMemory)
	if s.bootTime.After(cutoff) {
		cutoff = s.bootTime
	}

	opts := &github.ListCursorOptions{PerPage: 100}
	var scanned, rescued int
	for page := 0; page < deliveryPollMaxPages; page++ {
		ds, resp, err := gh.Apps.ListHookDeliveries(ctx, opts)
		if err != nil {
			return errors.Errorf("listing hook deliveries: %w", err)
		}

		inWindow := false
		for _, d := range ds {
			scanned++
			if d.ID == nil || d.GUID == nil || d.DeliveredAt == nil {
				continue
			}
			if d.DeliveredAt.Time.Before(cutoff) {
				continue
			}
			inWindow = true

			if s.deliveries.has(*d.GUID) {
				continue
			}

			log.Printf("[poll] delivery %d (%s) event=%s status=%q never reached us, processing it",
				*d.ID, *d.GUID, d.GetEvent(), d.GetStatus())
			if err := s.processMissedDelivery(ctx, gh, d); err != nil {
				// Leave the GUID unrecorded so the next tick tries again.
				// Jobs are only enqueued once an event has been fully handled,
				// so a retry can't half-duplicate a run.
				log.Printf("[poll] failed to process delivery %d: %v", *d.ID, err)
				continue
			}
			s.deliveries.add(*d.GUID)
			rescued++
		}

		// Deliveries aren't strictly ordered by delivered_at, and the timestamp
		// only has one-second granularity, so stopping at the first entry older
		// than the cutoff can skip records. Keep going until a whole page falls
		// outside the window.
		if !inWindow || resp.Cursor == "" {
			break
		}
		opts.Cursor = resp.Cursor
	}

	if rescued != 0 {
		log.Printf("[poll] scanned %d deliveries, rescued %d", scanned, rescued)
	}
	return nil
}

func (s *Service) processMissedDelivery(ctx context.Context, gh *github.Client, d *github.HookDelivery) error {
	// The list response carries only summaries; the payload needs a second
	// fetch per delivery.
	full, _, err := gh.Apps.GetHookDelivery(ctx, d.GetID())
	if err != nil {
		return errors.Errorf("fetching delivery %d: %w", d.GetID(), err)
	}
	if full.Request == nil || full.Request.RawPayload == nil {
		// GitHub stops storing the payload after a while. Nothing to replay.
		log.Printf("[poll] delivery %d has no stored payload, skipping", d.GetID())
		return nil
	}

	return s.processWebhook(ctx, d.GetEvent(), *full.Request.RawPayload)
}
