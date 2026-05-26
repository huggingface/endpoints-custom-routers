package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

var hopByHop = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailers":            true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Content-Length":      true,
	"Host":                true,
}

type srv struct {
	cfg      *config
	registry *BackendRegistry
	queue    *boundedQueue
	client   *http.Client
	m        *appMetrics
	wake     chan struct{}
}

type appMetrics struct {
	queueDepth      prometheus.Gauge
	backendEWMA     *prometheus.GaugeVec
	backendInFlight *prometheus.GaugeVec
	dispatched      prometheus.Counter
	evicted         prometheus.Counter
	timedOut        prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *appMetrics {
	m := &appMetrics{
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kvrouter_queue_depth",
			Help: "Number of requests waiting in the queue",
		}),
		backendEWMA: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kvrouter_backend_ewma_latency_seconds",
			Help: "EWMA latency per backend",
		}, []string{"addr"}),
		backendInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kvrouter_backend_inflight_requests",
			Help: "In-flight requests per backend",
		}, []string{"addr"}),
		dispatched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kvrouter_requests_dispatched_total",
			Help: "Requests successfully dispatched to a backend",
		}),
		evicted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kvrouter_requests_evicted_total",
			Help: "Requests dropped due to full queue (503)",
		}),
		timedOut: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kvrouter_requests_timeout_total",
			Help: "Requests dropped due to queue timeout (503)",
		}),
	}
	reg.MustRegister(m.queueDepth, m.backendEWMA, m.backendInFlight, m.dispatched, m.evicted, m.timedOut)
	return m
}

func main() {
	cfg := loadConfig()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	s := &srv{
		cfg:      cfg,
		registry: newBackendRegistry(cfg.ewmaAlpha, cfg.latencyThreshold, m.backendEWMA, m.backendInFlight),
		queue:    newBoundedQueue(cfg.queueMaxSize, m.queueDepth),
		client:   &http.Client{Timeout: 0}, // no timeout — backends handle their own
		m:        m,
		wake:     make(chan struct{}, 1),
	}

	go s.dispatcher()
	go s.periodicStateLog()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /_kvrouter/set-backends", s.handleSetBackends)
	mux.HandleFunc("GET /_kvrouter/health", s.handleHealth)
	mux.Handle("GET /_kvrouter/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/", s.handleProxy)

	logrus.SetLevel(logrus.InfoLevel)

	addr := fmt.Sprintf(":%d", cfg.port)
	logrus.WithFields(logrus.Fields{
		"port":        cfg.port,
		"threshold_s": cfg.latencyThreshold,
		"queue_max":   cfg.queueMaxSize,
		"timeout_s":   cfg.queueTimeout.Seconds(),
		"ewma_alpha":  cfg.ewmaAlpha,
	}).Info("kvrouter started")
	if err := http.ListenAndServe(addr, mux); err != nil {
		logrus.WithError(err).Fatal("server error")
		os.Exit(1)
	}
}

func (s *srv) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *srv) dispatcher() {
	var timeoutC <-chan time.Time
	for {
		select {
		case <-s.wake:
		case <-timeoutC:
		}
		timeoutC = nil
		for s.queue.len() > 0 {
			front := s.queue.peek()
			if time.Since(front.enqueuedAt) > s.cfg.queueTimeout {
				s.queue.pop()
				front.backendCh <- ""
				s.m.timedOut.Inc()
				continue
			}
			backend := s.registry.PickBest()
			if backend == "" {
				break
			}
			s.queue.pop()
			front.backendCh <- backend
			s.m.dispatched.Inc()
		}
		// wake up when the oldest item times out, if any remain
		if front := s.queue.peek(); front != nil {
			remaining := s.cfg.queueTimeout - time.Since(front.enqueuedAt)
			if remaining <= 0 {
				remaining = 0
			}
			timeoutC = time.After(remaining)
		}
	}
}

func (s *srv) handleProxy(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		http.Error(w, "error reading request body", http.StatusBadRequest)
		return
	}

	item := &queueItem{
		backendCh:  make(chan string, 1),
		enqueuedAt: time.Now(),
	}

	if evicted := s.queue.push(item); evicted != nil {
		evicted.backendCh <- ""
		s.m.evicted.Inc()
	}
	s.notify()

	backend := <-item.backendCh
	if backend == "" {
		http.Error(w, "queue full or timeout", http.StatusServiceUnavailable)
		return
	}

	logrus.WithFields(logrus.Fields{
		"backend":       backend,
		"method":        r.Method,
		"path":          r.URL.Path,
		"queue_wait_ms": time.Since(item.enqueuedAt).Milliseconds(),
	}).Info("dispatching request")

	start := time.Now()
	s.forward(w, r, backend, body)
	s.registry.RecordResult(backend, time.Since(start).Seconds())
	s.notify()
}

func (s *srv) forward(w http.ResponseWriter, r *http.Request, backend string, body []byte) {
	url := backend + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	copyHeaders(req.Header, r.Header)

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		logrus.WithFields(logrus.Fields{"backend": backend}).WithError(err).Error("forward error")
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	// Flush after each write so SSE and chunked streaming reaches the client immediately.
	flusher, canFlush := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			logrus.WithFields(logrus.Fields{"backend": backend}).WithError(readErr).Error("stream read error")
			return
		}
	}
}

func (s *srv) handleSetBackends(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Backends []string `json:"backends"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	s.registry.SetBackends(payload.Backends)
	s.notify()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (s *srv) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok":          true,
		"queue_depth": s.queue.len(),
		"backends":    s.registry.Stats(),
	})
}

func (s *srv) periodicStateLog() {
	ticker := time.NewTicker(s.cfg.stateLogInterval)
	defer ticker.Stop()
	for range ticker.C {
		queueDepth := s.queue.len()
		for _, b := range s.registry.Stats() {
			logrus.WithFields(logrus.Fields{
				"addr":        b["addr"],
				"ewma_s":      b["ewma_latency"],
				"in_flight":   b["in_flight"],
				"queue_depth": queueDepth,
			}).Info("backend state")
		}
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		if hopByHop[k] {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
