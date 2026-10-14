package main

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestScalingMetricCountsQueuedAndInFlightRequests(t *testing.T) {
	m := newMetrics(prometheus.NewRegistry())
	s := &srv{
		registry: newBackendRegistry(0.3, 3.0, 10, m.backendEWMA, m.backendInFlight),
		queue:    newBoundedQueue(10, m.queueDepth),
	}
	metric := newScalingMetric(s)
	if got := testutil.ToFloat64(metric); got != 0 {
		t.Fatalf("idle router: got %v, want 0", got)
	}

	s.registry.SetBackends([]string{"http://a:80", "http://b:80"})
	s.registry.PickBest()
	s.registry.PickBest()
	s.queue.push(&queueItem{})
	if got := testutil.ToFloat64(metric); got != 3 {
		t.Fatalf("2 in flight + 1 queued: got %v, want 3", got)
	}

	s.registry.RecordResult("http://a:80", 0.1)
	s.queue.pop()
	if got := testutil.ToFloat64(metric); got != 1 {
		t.Fatalf("1 in flight: got %v, want 1", got)
	}
}
