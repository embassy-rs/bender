package main

import (
	"testing"
	"time"

	"github.com/google/go-github/v72/github"
)

func TestJobQueue(t *testing.T) {
	// Create a test service with minimal config
	s := &Service{
		config: Config{
			MaxConcurrency: 2,
		},
		runningJobs: make(map[string]*Job),
		jobQueue:    NewJobQueue(),
	}

	// Test queue status initially
	queued, running := s.getQueueStatus()
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
	go s.enqueueJob(job)

	// Give it a moment to be enqueued
	time.Sleep(10 * time.Millisecond)

	// Check queue status
	queued, running = s.getQueueStatus()
	if queued != 1 {
		t.Errorf("Expected 1 queued job, got %d", queued)
	}
	if running != 0 {
		t.Errorf("Expected 0 running jobs, got %d", running)
	}
}

func TestJobPriorityQueue(t *testing.T) {
	jq := NewJobQueue()

	// Create jobs with different priorities
	job1 := &Job{ID: "job1", Name: "low", Priority: 1, Event: &Event{}}
	job2 := &Job{ID: "job2", Name: "high", Priority: 10, Event: &Event{}}
	job3 := &Job{ID: "job3", Name: "medium", Priority: 5, Event: &Event{}}
	job4 := &Job{ID: "job4", Name: "low-again", Priority: 1, Event: &Event{}}

	// Enqueue in random order
	jq.Enqueue(job1)
	jq.Enqueue(job2)
	jq.Enqueue(job3)
	jq.Enqueue(job4)

	// Should dequeue in priority order: job2 (10), job3 (5), job1 (1), job4 (1)
	dequeued1 := jq.Dequeue()
	if dequeued1.ID != "job2" {
		t.Errorf("Expected job2 (highest priority) first, got %s", dequeued1.ID)
	}

	dequeued2 := jq.Dequeue()
	if dequeued2.ID != "job3" {
		t.Errorf("Expected job3 (medium priority) second, got %s", dequeued2.ID)
	}

	dequeued3 := jq.Dequeue()
	if dequeued3.ID != "job1" {
		t.Errorf("Expected job1 (first low priority) third, got %s", dequeued3.ID)
	}

	dequeued4 := jq.Dequeue()
	if dequeued4.ID != "job4" {
		t.Errorf("Expected job4 (second low priority) fourth, got %s", dequeued4.ID)
	}
}

func TestDedupDequeueWaiting(t *testing.T) {
	// Create a test service
	s := &Service{
		config: Config{
			MaxConcurrency: 1, // Only 1 concurrent job to test waiting
		},
		runningJobs:        make(map[string]*Job),
		runningJobsByDedup: make(map[string]*Job),
		waitingJobs:        make(map[string][]*Job),
		jobQueue:           NewJobQueue(),
	}

	// Create two jobs with the same dedup key
	job1 := &Job{
		Event: &Event{
			Attributes: map[string]string{"branch": "main"},
			Repo: &github.Repository{
				Owner: &github.User{Login: github.Ptr("owner")},
				Name:  github.Ptr("repo"),
			},
		},
		ID:    "job1",
		Name:  "test",
		Dedup: DedupDequeue,
	}

	job2 := &Job{
		Event: &Event{
			Attributes: map[string]string{"branch": "main"},
			Repo: &github.Repository{
				Owner: &github.User{Login: github.Ptr("owner")},
				Name:  github.Ptr("repo"),
			},
		},
		ID:    "job2",
		Name:  "test",
		Dedup: DedupDequeue,
	}

	// Simulate job1 already running
	dedupKey := job1.DedupKey()
	s.runningJobsMutex.Lock()
	s.runningJobs[job1.ID] = job1
	s.runningJobsByDedup[dedupKey] = job1
	s.runningJobsMutex.Unlock()

	// Enqueue job2 - it should wait since job1 is running with same dedup key
	s.enqueueJob(job2)

	// Check that job2 is in waiting list
	s.runningJobsMutex.Lock()
	waitingJobs := s.waitingJobs[dedupKey]
	s.runningJobsMutex.Unlock()

	if len(waitingJobs) != 1 {
		t.Errorf("Expected 1 waiting job, got %d", len(waitingJobs))
	}
	if waitingJobs[0].ID != job2.ID {
		t.Errorf("Expected waiting job to be job2, got %s", waitingJobs[0].ID)
	}

	// Check that job2 is not in the main queue
	if s.jobQueue.Len() != 0 {
		t.Errorf("Expected main queue to be empty, got %d jobs", s.jobQueue.Len())
	}

	// Simulate job1 finishing
	s.enqueueWaitingJobs(dedupKey)

	// Check that job2 is now in the main queue
	if s.jobQueue.Len() != 1 {
		t.Errorf("Expected 1 job in main queue after waiting job enqueued, got %d", s.jobQueue.Len())
	}

	// Check that waiting list is empty
	s.runningJobsMutex.Lock()
	waitingJobs = s.waitingJobs[dedupKey]
	s.runningJobsMutex.Unlock()

	if len(waitingJobs) != 0 {
		t.Errorf("Expected waiting list to be empty after enqueuing, got %d jobs", len(waitingJobs))
	}
}

func TestDedupDequeueNoWaitingWhenNoRunning(t *testing.T) {
	// Create a test service
	s := &Service{
		config: Config{
			MaxConcurrency: 2,
		},
		runningJobs:        make(map[string]*Job),
		runningJobsByDedup: make(map[string]*Job),
		waitingJobs:        make(map[string][]*Job),
		jobQueue:           NewJobQueue(),
	}

	// Create a job with dedup=dequeue
	job := &Job{
		Event: &Event{
			Attributes: map[string]string{"branch": "main"},
			Repo: &github.Repository{
				Owner: &github.User{Login: github.Ptr("owner")},
				Name:  github.Ptr("repo"),
			},
		},
		ID:    "job1",
		Name:  "test",
		Dedup: DedupDequeue,
	}

	// Enqueue job when no duplicate is running
	s.enqueueJob(job)

	// Check that job goes directly to main queue (no waiting)
	if s.jobQueue.Len() != 1 {
		t.Errorf("Expected 1 job in main queue, got %d", s.jobQueue.Len())
	}

	// Check that waiting list is empty
	dedupKey := job.DedupKey()
	s.runningJobsMutex.Lock()
	waitingJobs := s.waitingJobs[dedupKey]
	s.runningJobsMutex.Unlock()

	if len(waitingJobs) != 0 {
		t.Errorf("Expected no waiting jobs, got %d", len(waitingJobs))
	}
}
