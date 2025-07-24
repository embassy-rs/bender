package main

import (
	"container/heap"
	"sync"
	"time"
)

// JobQueueItem wraps a Job with metadata for the priority queue
type JobQueueItem struct {
	Job       *Job
	Priority  int
	Timestamp time.Time // For FIFO ordering when priorities are equal
	Index     int       // Required by heap.Interface
}

// JobPriorityQueue implements a priority queue for jobs
type JobPriorityQueue []*JobQueueItem

func (pq JobPriorityQueue) Len() int { return len(pq) }

func (pq JobPriorityQueue) Less(i, j int) bool {
	// Higher priority first
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	// If priorities are equal, earlier timestamp first (FIFO)
	return pq[i].Timestamp.Before(pq[j].Timestamp)
}

func (pq JobPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *JobPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*JobQueueItem)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *JobPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// JobQueue wraps the priority queue with thread safety
type JobQueue struct {
	pq    JobPriorityQueue
	mutex sync.Mutex
	cond  *sync.Cond
}

func NewJobQueue() *JobQueue {
	jq := &JobQueue{
		pq: make(JobPriorityQueue, 0),
	}
	jq.cond = sync.NewCond(&jq.mutex)
	heap.Init(&jq.pq)
	return jq
}

func (jq *JobQueue) Enqueue(job *Job) {
	jq.mutex.Lock()
	defer jq.mutex.Unlock()

	item := &JobQueueItem{
		Job:       job,
		Priority:  job.Priority,
		Timestamp: time.Now(),
	}

	heap.Push(&jq.pq, item)
	jq.cond.Signal()
}

func (jq *JobQueue) Dequeue() *Job {
	jq.mutex.Lock()
	defer jq.mutex.Unlock()

	for len(jq.pq) == 0 {
		jq.cond.Wait()
	}

	item := heap.Pop(&jq.pq).(*JobQueueItem)
	return item.Job
}

func (jq *JobQueue) Len() int {
	jq.mutex.Lock()
	defer jq.mutex.Unlock()
	return len(jq.pq)
}

// RemoveByDedupKey removes all jobs from the queue that have the given dedup key
func (jq *JobQueue) RemoveByDedupKey(dedupKey string) int {
	jq.mutex.Lock()
	defer jq.mutex.Unlock()

	removed := 0
	// Iterate through the queue and remove items with matching dedup key
	for i := len(jq.pq) - 1; i >= 0; i-- {
		if jq.pq[i].Job.DedupKey() == dedupKey {
			heap.Remove(&jq.pq, i)
			removed++
		}
	}

	return removed
}

// GetAllJobs returns a copy of all jobs currently in the queue
func (jq *JobQueue) GetAllJobs() []*Job {
	jq.mutex.Lock()
	defer jq.mutex.Unlock()

	jobs := make([]*Job, len(jq.pq))
	for i, item := range jq.pq {
		jobs[i] = item.Job
	}
	return jobs
}
