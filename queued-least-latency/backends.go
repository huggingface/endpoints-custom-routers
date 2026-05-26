package main

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const rampUpFactor = 5

type backendState struct {
	ewmaLatency *float64 // nil = never tried; treated as available (optimistic)
	inFlight    int
	completed   int
}

type BackendRegistry struct {
	alpha        float64
	threshold    float64
	maxCompleted int // caps the ramp-up counter; 5*maxCompleted = 2*queueMaxSize
	backends     map[string]*backendState
	mu           sync.Mutex
	latency      *prometheus.GaugeVec
	inFlight     *prometheus.GaugeVec
}

func newBackendRegistry(alpha, threshold float64, queueMaxSize int, latency, inFlight *prometheus.GaugeVec) *BackendRegistry {
	return &BackendRegistry{
		alpha:        alpha,
		threshold:    threshold,
		maxCompleted: 2 * queueMaxSize / rampUpFactor,
		backends:     make(map[string]*backendState),
		latency:      latency,
		inFlight:     inFlight,
	}
}

func (r *BackendRegistry) SetBackends(addrs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make(map[string]*backendState, len(addrs))
	for _, addr := range addrs {
		if s, ok := r.backends[addr]; ok {
			next[addr] = s // preserve existing EWMA
		} else {
			next[addr] = &backendState{}
		}
	}
	r.backends = next
	logrus.WithField("addrs", addrs).Info("backends updated")
}

// PickBest returns the backend with the lowest EWMA latency that is under the
// threshold. A backend above the threshold is still eligible if it has no
// in-flight requests — there is no point holding the queue when the backend
// is idle regardless of its historical latency.
func (r *BackendRegistry) PickBest() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	best := ""
	bestLatency := -1.0

	for addr, s := range r.backends {
		var lat float64
		if s.ewmaLatency == nil {
			if s.inFlight == 0 {
				lat = 0 // untried and idle — prefer it for probing
			} else {
				continue // already being probed — wait for result before sending more
			}
		} else if s.inFlight == 0 {
			lat = *s.ewmaLatency // idle — always use regardless of threshold
		} else if *s.ewmaLatency >= r.threshold || s.inFlight >= rampUpFactor*s.completed {
			continue // above threshold or ramp-up cap not yet reached
		} else {
			lat = *s.ewmaLatency
		}
		if best == "" || lat < bestLatency {
			best = addr
			bestLatency = lat
		}
	}

	if best != "" {
		r.backends[best].inFlight++
		r.inFlight.WithLabelValues(best).Inc()
	}
	return best
}

func (r *BackendRegistry) RecordResult(addr string, duration float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.backends[addr]
	if !ok {
		return
	}
	if s.inFlight > 0 {
		s.inFlight--
		r.inFlight.WithLabelValues(addr).Dec()
	}
	if s.completed < r.maxCompleted {
		s.completed++
	}
	if s.ewmaLatency == nil {
		s.ewmaLatency = &duration
	} else {
		v := r.alpha*duration + (1-r.alpha)*(*s.ewmaLatency)
		s.ewmaLatency = &v
	}
	r.latency.WithLabelValues(addr).Set(*s.ewmaLatency)
	logrus.WithFields(logrus.Fields{"addr": addr, "ewma_s": *s.ewmaLatency}).Debug("ewma updated")
}

func (r *BackendRegistry) Stats() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := make([]map[string]any, 0, len(r.backends))
	for addr, s := range r.backends {
		m := map[string]any{"addr": addr, "in_flight": s.inFlight, "ewma_latency": nil}
		if s.ewmaLatency != nil {
			m["ewma_latency"] = *s.ewmaLatency
		}
		stats = append(stats, m)
	}
	return stats
}
