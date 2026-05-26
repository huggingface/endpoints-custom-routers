package main

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type queueItem struct {
	backendCh  chan string // receives assigned backend addr, or "" for eviction/timeout
	enqueuedAt time.Time
}

type boundedQueue struct {
	items   []*queueItem
	maxSize int
	mu      sync.Mutex
	depth   prometheus.Gauge
}

func newBoundedQueue(maxSize int, depth prometheus.Gauge) *boundedQueue {
	return &boundedQueue{maxSize: maxSize, depth: depth}
}

// push enqueues item. If the queue is at capacity the oldest item is evicted and returned;
// the caller is responsible for sending "" on its backendCh to trigger a 503.
func (q *boundedQueue) push(item *queueItem) (evicted *queueItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.maxSize {
		evicted = q.items[0]
		q.items = q.items[1:]
		q.depth.Dec()
	}
	q.items = append(q.items, item)
	q.depth.Inc()
	return
}

// peek returns the oldest item without removing it, or nil if empty.
func (q *boundedQueue) peek() *queueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}

// pop removes and returns the oldest item. Caller must ensure the queue is non-empty.
func (q *boundedQueue) pop() *queueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	item := q.items[0]
	q.items = q.items[1:]
	q.depth.Dec()
	return item
}

func (q *boundedQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
