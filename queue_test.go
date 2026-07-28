package main

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v88/github"
)

func TestSimplifiedJobQueue(t *testing.T) {
	// Create a test queue
	queue := &Queue{
		jobs:           make([]*Job, 0),
		maxConcurrency: 2,
	}
	queue.schedulerCond = sync.NewCond(&queue.mutex)

	// Create a test service with minimal config
	s := &Service{
		config: Config{
			MaxConcurrency: 2,
		},
		queue: queue,
	}

	// Test queue status initially
	queued, running := s.queue.getQueueStatus()
	if queued != 0 {
		t.Errorf("Expected 0 queued jobs, got %d", queued)
	}
	if running != 0 {
		t.Errorf("Expected 0 running jobs, got %d", running)
	}

	// Create a test job
	job := &Job{
		ID:       "test-job-1",
		Name:     "test",
		Priority: 0,
		Event:    &Event{},
	}

	// Enqueue a job
	s.queue.enqueueJob(job, s)

	// Check queue status
	queued, running = s.queue.getQueueStatus()
	if queued != 1 {
		t.Errorf("Expected 1 queued job, got %d", queued)
	}
	if running != 0 {
		t.Errorf("Expected 0 running jobs, got %d", running)
	}

	// Check that the job is in queued state
	if job.State != JobStateQueued {
		t.Errorf("Expected job state to be queued, got %v", job.State)
	}
}

func TestJobPriorityOrdering(t *testing.T) {
	// Create a test queue
	queue := &Queue{
		jobs:           make([]*Job, 0),
		maxConcurrency: 1,
	}
	queue.schedulerCond = sync.NewCond(&queue.mutex)

	s := &Service{
		config: Config{
			MaxConcurrency: 1,
		},
		queue: queue,
	}

	// Create jobs with different priorities
	job1 := &Job{ID: "job1", Name: "low", Priority: 1, Event: &Event{}}
	job2 := &Job{ID: "job2", Name: "high", Priority: 10, Event: &Event{}}
	job3 := &Job{ID: "job3", Name: "medium", Priority: 5, Event: &Event{}}

	// Enqueue in random order
	s.queue.enqueueJob(job1, s)
	s.queue.enqueueJob(job2, s)
	s.queue.enqueueJob(job3, s)

	// Check that findJobToStart picks the highest priority job
	queue.mutex.Lock()
	jobToStart := queue.findJobToStart()
	queue.mutex.Unlock()

	if jobToStart == nil {
		t.Errorf("Expected to find a job to start")
	} else if jobToStart.ID != "job2" {
		t.Errorf("Expected job2 (highest priority) to be selected, got %s", jobToStart.ID)
	}
}

func TestDeduplication(t *testing.T) {
	// Create a test queue
	queue := &Queue{
		jobs:           make([]*Job, 0),
		maxConcurrency: 2,
	}
	queue.schedulerCond = sync.NewCond(&queue.mutex)

	s := &Service{
		config: Config{
			MaxConcurrency: 2,
		},
		queue: queue,
	}

	// Create jobs with same dedup key
	job1 := &Job{
		ID:       "job1",
		Name:     "test",
		Priority: 1,
		Dedup:    DedupKill,
		Event: &Event{
			Repo: &github.Repository{
				Owner: &github.User{Login: github.Ptr("owner")},
				Name:  github.Ptr("repo"),
			},
		},
	}

	job2 := &Job{
		ID:       "job2",
		Name:     "test",
		Priority: 1,
		Dedup:    DedupKill,
		Event: &Event{
			Repo: &github.Repository{
				Owner: &github.User{Login: github.Ptr("owner")},
				Name:  github.Ptr("repo"),
			},
		},
	}

	// Enqueue first job
	s.queue.enqueueJob(job1, s)

	// Set first job to running state
	queue.mutex.Lock()
	job1.State = JobStateRunning
	queue.mutex.Unlock()

	// Enqueue second job with same dedup key
	s.queue.enqueueJob(job2, s)

	// First job should be cancelled due to deduplication
	time.Sleep(10 * time.Millisecond) // Give time for cancel to propagate
}

func TestJobStates(t *testing.T) {
	// Create a test queue
	queue := &Queue{
		jobs:           make([]*Job, 0),
		maxConcurrency: 1,
	}
	queue.schedulerCond = sync.NewCond(&queue.mutex)

	s := &Service{
		config: Config{
			MaxConcurrency: 1,
		},
		queue: queue,
	}

	job := &Job{
		ID:       "test-job",
		Name:     "test",
		Priority: 1,
		Event:    &Event{},
	}

	// Test initial state after enqueue
	s.queue.enqueueJob(job, s)
	if job.State != JobStateQueued {
		t.Errorf("Expected job state to be queued, got %v", job.State)
	}

	// Test running state
	queue.mutex.Lock()
	job.State = JobStateRunning
	job.StartedAt = time.Now()
	queue.mutex.Unlock()

	if job.State != JobStateRunning {
		t.Errorf("Expected job state to be running, got %v", job.State)
	}

	// Test job finished
	s.queue.onJobFinished(job)

	// Job should be removed from the list
	allJobs := s.queue.getAllJobs()
	if len(allJobs) != 0 {
		t.Errorf("Expected no jobs after finish, got %d", len(allJobs))
	}
}

func newTestQueue() *Queue {
	q := &Queue{jobs: make([]*Job, 0), maxConcurrency: 2}
	q.schedulerCond = sync.NewCond(&q.mutex)
	return q
}

// A canceled job must carry why it died, so runJob and the GitHub status can
// report it instead of a bare "context canceled".
func TestCancelReasonIsRecorded(t *testing.T) {
	t.Run("queued job via killJobs", func(t *testing.T) {
		q := newTestQueue()
		job := &Job{ID: "a", Name: "test", State: JobStateQueued, Event: &Event{}}
		q.jobs = append(q.jobs, job)

		dropped := q.killJobs("PR #7 was closed", func(j *Job) bool { return true })

		// Queued jobs are returned so the caller can clear their pending status.
		if len(dropped) != 1 || dropped[0] != job {
			t.Fatalf("got %d dropped jobs, want the one queued job", len(dropped))
		}
		if got := q.cancelReason(job); got != "PR #7 was closed" {
			t.Errorf("cancelReason = %q, want %q", got, "PR #7 was closed")
		}
		if len(q.jobs) != 0 {
			t.Errorf("job still in queue after kill")
		}
	})

	t.Run("running job via killJobs", func(t *testing.T) {
		q := newTestQueue()
		canceled := false
		job := &Job{ID: "b", Name: "test", State: JobStateRunning, Event: &Event{},
			cancelFunc: func() { canceled = true }}
		q.jobs = append(q.jobs, job)

		dropped := q.killJobs("branch foo was deleted", func(j *Job) bool { return true })

		// A running job reports its own status via runJob, so it isn't returned.
		if len(dropped) != 0 {
			t.Errorf("got %d dropped jobs, want 0 for a running job", len(dropped))
		}
		if !canceled {
			t.Error("running job was not canceled")
		}
		if got := q.cancelReason(job); got != "branch foo was deleted" {
			t.Errorf("cancelReason = %q, want %q", got, "branch foo was deleted")
		}
	})

	t.Run("cancelJob reports whether it was queued", func(t *testing.T) {
		q := newTestQueue()
		queued := &Job{ID: "c", Name: "test", State: JobStateQueued, Event: &Event{}}
		running := &Job{ID: "d", Name: "test", State: JobStateRunning, Event: &Event{},
			cancelFunc: func() {}}
		q.jobs = append(q.jobs, queued, running)

		got, wasQueued := q.cancelJob("c", "requested by @alice")
		if got != queued || !wasQueued {
			t.Errorf("cancelJob(c) = %v, %v; want the queued job, true", got, wasQueued)
		}
		if r := q.cancelReason(queued); r != "requested by @alice" {
			t.Errorf("cancelReason = %q, want %q", r, "requested by @alice")
		}

		got, wasQueued = q.cancelJob("d", "requested by @bob")
		if got != running || wasQueued {
			t.Errorf("cancelJob(d) = %v, %v; want the running job, false", got, wasQueued)
		}

		if got, _ := q.cancelJob("nonexistent", "whatever"); got != nil {
			t.Errorf("cancelJob(nonexistent) = %v, want nil", got)
		}
	})

	t.Run("uncanceled job has no reason", func(t *testing.T) {
		q := newTestQueue()
		job := &Job{ID: "e", Name: "test", State: JobStateRunning, Event: &Event{}}
		if got := q.cancelReason(job); got != "" {
			t.Errorf("cancelReason = %q, want empty", got)
		}
	})
}
