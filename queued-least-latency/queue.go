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
	cond    *sync.Cond
	depth   prometheus.Gauge
}

func newBoundedQueue(maxSize int, depth prometheus.Gauge) *boundedQueue {
	q := &boundedQueue{maxSize: maxSize, depth: depth}
	q.cond = sync.NewCond(&q.mu)
	return q
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
	q.cond.Signal()
	return
}

// pop blocks until an item is available, then returns the oldest item.
func (q *boundedQueue) pop() *queueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		q.cond.Wait()
	}
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
