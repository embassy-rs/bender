package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"
)

type Queue struct {
	// Job management
	mutex sync.Mutex
	jobs  []*Job // All jobs (queued, running)

	// Scheduler signaling
	schedulerCond *sync.Cond

	// Configuration
	maxConcurrency int
}

// newQueue creates a new Queue with the specified maximum concurrency
func newQueue(maxConcurrency int) *Queue {
	queue := &Queue{
		jobs:           make([]*Job, 0),
		maxConcurrency: maxConcurrency,
	}
	queue.schedulerCond = sync.NewCond(&queue.mutex)
	return queue
}

// enqueueJob adds a job to the queue and signals the scheduler
func (q *Queue) enqueueJob(job *Job, s *Service) {
	job.State = JobStateQueued
	job.EnqueuedAt = time.Now()

	queuedJobs, runningJobs := q.getQueueStatus()
	log.Printf("Enqueuing job %s (%s) [Priority: %d, Dedup: %s] - Queue: %d jobs, Running: %d/%d",
		job.ID, job.Name, job.Priority, job.Dedup, queuedJobs, runningJobs, q.maxConcurrency)

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

	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Handle deduplication
	if job.Dedup != DedupNone {
		q.handleDeduplication(job)
	}

	q.jobs = append(q.jobs, job)
	q.schedulerCond.Signal()
}

// handleDeduplication processes deduplication logic for a new job
func (q *Queue) handleDeduplication(newJob *Job) {
	dedupKey := newJob.DedupKey()

	for _, existingJob := range q.jobs {
		if existingJob.DedupKey() == dedupKey {
			if existingJob.State == JobStateQueued {
				log.Printf("Removing queued job %s due to deduplication", existingJob.ID)
				q.removeJobUnsafe(existingJob.ID)
			} else if newJob.Dedup == DedupKill {
				log.Printf("Killing running job %s due to deduplication", existingJob.ID)
				existingJob.Cancel()
			}
		}
	}
}

// removeJobUnsafe removes a job from the slice without locking (caller must hold lock)
func (q *Queue) removeJobUnsafe(jobID string) {
	for i, job := range q.jobs {
		if job.ID == jobID {
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			break
		}
	}
}

func (q *Queue) nextJob() *Job {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	var job *Job
	for {
		job = q.findJobToStart()
		if job != nil {
			break
		}
		// Wait for signal if no jobs can be started
		q.schedulerCond.Wait()
	}

	// start it!
	log.Printf("Starting job %s (%s)", job.ID, job.Name)
	job.State = JobStateRunning
	job.StartedAt = time.Now()

	return job
}

// findJobToStart finds the best job to start based on priority and wait time
// Returns nil if no job can start (handles both dedup logic and candidate selection)
func (q *Queue) findJobToStart() *Job {
	runningCount := q.countJobsByState(JobStateRunning)
	if runningCount >= q.maxConcurrency {
		return nil
	}

	var candidates []*Job

	// Collect all jobs that can start
	for _, job := range q.jobs {
		if job.State != JobStateQueued {
			continue
		}

		// Check deduplication constraints
		if job.Dedup != DedupNone {
			dedupKey := job.DedupKey()
			hasRunningDuplicate := false
			for _, otherJob := range q.jobs {
				if otherJob.State == JobStateRunning && otherJob.DedupKey() == dedupKey {
					hasRunningDuplicate = true
					break
				}
			}
			if hasRunningDuplicate {
				continue
			}
		}

		candidates = append(candidates, job)
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort by priority (higher first), then by enqueue time (earlier first)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		return candidates[i].EnqueuedAt.Before(candidates[j].EnqueuedAt)
	})

	return candidates[0]
}

// countJobsByState counts jobs in a specific state
func (q *Queue) countJobsByState(state JobState) int {
	count := 0
	for _, job := range q.jobs {
		if job.State == state {
			count++
		}
	}
	return count
}

// onJobFinished is called when a job finishes (successfully or not)
func (q *Queue) onJobFinished(job *Job) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	log.Printf("Finished job %s (%s)", job.ID, job.Name)

	// Remove the job from the list
	q.removeJobUnsafe(job.ID)

	// Signal the scheduler to check for new jobs to start
	// (jobs that were blocked by dedup can now potentially start)
	q.schedulerCond.Signal()
}

// getQueueStatus returns the current queue status
func (q *Queue) getQueueStatus() (queuedJobs int, runningJobs int) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	queuedJobs = q.countJobsByState(JobStateQueued)
	runningJobs = q.countJobsByState(JobStateRunning)
	return queuedJobs, runningJobs
}

// isJobRunning checks if a job is currently running
func (q *Queue) isJobRunning(id string) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for _, job := range q.jobs {
		if job.ID == id && job.State == JobStateRunning {
			return true
		}
	}
	return false
}

// getAllJobs returns all jobs (for dashboard)
func (q *Queue) getAllJobs() []*Job {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Return a copy of the slice to avoid race conditions
	jobs := make([]*Job, len(q.jobs))
	copy(jobs, q.jobs)
	return jobs
}
