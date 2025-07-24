package main

import (
	"testing"
	"time"
)

func TestJobQueue(t *testing.T) {
	// Create a test service with minimal config
	s := &Service{
		config: Config{
			MaxConcurrency: 2,
		},
		runningJobs: make(map[string]struct{}),
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
