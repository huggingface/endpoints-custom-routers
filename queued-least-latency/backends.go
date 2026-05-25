package main

import (
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type backendState struct {
	ewmaLatency *float64 // nil = never tried; treated as available (optimistic)
	inFlight    int
}

type BackendRegistry struct {
	alpha     float64
	threshold float64
	backends  map[string]*backendState
	mu        sync.Mutex
	latency   *prometheus.GaugeVec
}

func newBackendRegistry(alpha, threshold float64, latency *prometheus.GaugeVec) *BackendRegistry {
	return &BackendRegistry{
		alpha:     alpha,
		threshold: threshold,
		backends:  make(map[string]*backendState),
		latency:   latency,
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
	slog.Info("backends updated", "addrs", addrs)
}

// PickBest returns the address of the available backend with the lowest EWMA latency,
// or "" if all backends are above the threshold or none are registered.
// Never-tried backends are treated as latency 0 (tried first, optimistic).
func (r *BackendRegistry) PickBest() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	best := ""
	bestLatency := -1.0

	for addr, s := range r.backends {
		var lat float64
		if s.ewmaLatency == nil {
			lat = 0
		} else if *s.ewmaLatency < r.threshold {
			lat = *s.ewmaLatency
		} else {
			continue
		}
		if best == "" || lat < bestLatency {
			best = addr
			bestLatency = lat
		}
	}

	if best != "" {
		r.backends[best].inFlight++
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
	}
	if s.ewmaLatency == nil {
		s.ewmaLatency = &duration
	} else {
		v := r.alpha*duration + (1-r.alpha)*(*s.ewmaLatency)
		s.ewmaLatency = &v
	}
	r.latency.WithLabelValues(addr).Set(*s.ewmaLatency)
	slog.Debug("ewma updated", "addr", addr, "ewma", *s.ewmaLatency)
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
