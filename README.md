# kv-routers

A collection of load balancing routers for the kv-router experimental feature in Hugging Face Inference Endpoints. Each subdirectory implements a distinct routing strategy as a standalone HTTP proxy.

## How the kv-router feature works

The kv-router is an Endpoints-specific feature. When enabled on an endpoint, a sidecar container is injected into the replicas. Traffic flows like this:

```
External request
      ↓
  Endpoints proxy  (always forwards to the leader pod)
      ↓
  KV router sidecar  (port 3000 — makes the actual routing decision)
      ↓
  Target replica  (same pod or a peer, via inter-pod network)
```

The proxy calls `POST /_kvrouter/set-backends` whenever the replica set changes to keep the sidecar up to date. The sidecar is a **black box** from the platform's perspective — any load balancing strategy can be implemented as a custom image, as long as it respects the two-endpoint contract:

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/_kvrouter/set-backends` | Receive current backend pod IPs: `{"backends": ["host:port", ...]}` |
| `GET` | `/_kvrouter/health` | Readiness probe |

## Strategies

### `queued-least-latency`

Routes requests to the backend with the lowest observed latency, using a FIFO queue to absorb bursts when all backends are busy.

**How it works:**

1. Every incoming request is pushed onto a bounded FIFO queue.
2. A single dispatcher goroutine pulls requests from the queue and assigns each one to the *best available* backend — the one with the lowest [EWMA](https://en.wikipedia.org/wiki/Exponential_smoothing) latency that is still below the configured threshold.
3. Never-tried backends are treated as latency 0 and picked first (optimistic initial routing).
4. If no backend is below the threshold, the dispatcher retries every 50 ms until one becomes available or the per-request queue timeout elapses.
5. Actual end-to-end latency is measured after each response and fed back into the EWMA for that backend.

**Backpressure:**

- When the queue is full, the oldest waiting request is evicted (503).
- When a request has waited longer than `KVROUTER_QUEUE_TIMEOUT`, it is dropped (503).

**Configuration (environment variables):**

| Variable | Default | Description |
|---|---|---|
| `KVROUTER_PORT` | `3000` | Listening port |
| `KVROUTER_LATENCY_THRESHOLD` | `3.0` | Max EWMA latency (seconds) for a backend to be considered available |
| `KVROUTER_QUEUE_MAX_SIZE` | `1000` | Maximum number of requests held in the queue |
| `KVROUTER_QUEUE_TIMEOUT` | `1200` | Seconds a request may wait in the queue before being dropped |
| `KVROUTER_EWMA_ALPHA` | `0.3` | EWMA smoothing factor (higher = more reactive to recent latency) |

**API:**

| Method | Path | Description |
|---|---|---|
| `POST` | `/_kvrouter/set-backends` | Set the backend list: `{"backends": ["host:port", ...]}` |
| `GET` | `/_kvrouter/health` | Returns queue depth and per-backend EWMA stats |
| `GET` | `/_kvrouter/metrics` | Prometheus metrics |
| `*` | `/` | Proxied to the selected backend (streaming-aware) |

**Prometheus metrics:**

| Metric | Type | Description |
|---|---|---|
| `kvrouter_queue_depth` | Gauge | Current number of requests waiting in the queue |
| `kvrouter_backend_ewma_latency_seconds` | Gauge | EWMA latency per backend (`addr` label) |
| `kvrouter_requests_dispatched_total` | Counter | Requests successfully forwarded |
| `kvrouter_requests_evicted_total` | Counter | Requests dropped due to full queue |
| `kvrouter_requests_timeout_total` | Counter | Requests dropped due to queue timeout |

**Running with Docker:**

```bash
docker build -t kvrouter ./queued-least-latency
docker run -e KVROUTER_LATENCY_THRESHOLD=2.0 -p 3000:3000 kvrouter
```
