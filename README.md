# High-Throughput Webhook & Notification Dispatcher (Go)

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://golang.org)
[![gRPC](https://img.shields.io/badge/gRPC-v1.65-244c5a?style=for-the-badge&logo=grpc&logoColor=white)](https://grpc.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-pgx%2Fv5-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://github.com/jackc/pgx)
[![Redis](https://img.shields.io/badge/Redis-ZSET%20Scheduler-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io)
[![Echo Framework](https://img.shields.io/badge/Echo-v4-00b4d8?style=for-the-badge&logo=fastapi&logoColor=white)](https://echo.labstack.com)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io)

A production-grade, distributed Go microservice engineered to reliably deliver high-volume, real-time payment transactions and in-game notifications to external game studios and partner platforms.

---

## 📸 Screenshots & Interactive Dashboard

The service includes an interactive, responsive Web UI Dashboard for real-time monitoring, endpoint configuration, event simulation, and 1-click Dead Letter Queue (DLQ) replaying.

### 1. Webhook Management & Live Dispatch Dashboard
![Webhook Dispatcher Dashboard](docs/screenshots/dashboard_overview.png)

### 2. Real-Time Prometheus Metrics Exposure
![Prometheus Metrics Endpoint](docs/screenshots/metrics_prometheus.png)

---

## 🚀 Key Architectural Highlights

- **Language & Runtime:** Built with **Go (1.23+)** taking full advantage of Go's native concurrency primitives: goroutines, buffered channels, sync primitives, and atomic memory operations.
- **Internal API:** Ultra-low latency **gRPC** server with Protocol Buffers (`google.golang.org/grpc`, `google.golang.org/protobuf`) for high-throughput event ingestion.
- **High-Concurrency Worker Pool:** Bounded worker pool (configurable, e.g. 50 concurrent worker goroutines) reading from buffered channels with tuned HTTP/2 pooled transports, strict connection timeouts (5s connect, 10s total), and graceful draining on shutdown.
- **Cryptographic Security:** Payload integrity guaranteed with **HMAC-SHA256** signatures (`X-Webhook-Signature: sha256=<hex>`), verified using constant-time comparison (`crypto/subtle.ConstantTimeCompare`) against timing side-channel attacks.
- **Fault-Tolerant Exponential Backoff with Jitter:** Non-2xx HTTP responses or timeouts trigger exponential backoff with full jitter to avoid thundering herd surges.
- **Delayed Retry Scheduling with Redis Sorted Sets:** Retries are stored in Redis ZSETs (`ZADD`) with their next execution millisecond timestamp as the score. Atomic Lua scripts claim due jobs across distributed replicas.
- **Dead Letter Queue (DLQ) & Manual Replay:** Events failing all retry attempts are marked as `FAILED` and recorded in the PostgreSQL `dead_letter_queue` table, with a manual replay REST endpoint (`POST /api/v1/dlq/:id/replay`).
- **Observability:** Prometheus metrics (`/metrics`) and structured JSON logging (`log/slog`).
- **Resilient Storage Architecture:** Seamlessly connects to PostgreSQL + Redis in production or Docker, and gracefully defaults to in-memory storage for immediate local development.

---

## 📐 Architecture & Lifecycle Sequence

```mermaid
sequenceDiagram
    autonumber
    actor GameService as Game Engine / Billing Service
    participant gRPC as gRPC Server (:50051)
    participant PG as PostgreSQL (Events, Attempts, DLQ)
    participant Dispatcher as Dispatcher Worker Pool (50 Goroutines)
    participant Redis as Redis (ZSET Retry & Rate Limiter)
    participant Target as External Game Studio Server
    participant Admin as HTTP Admin & Replay API (:8080)

    %% Ingestion
    Note over GameService,gRPC: Phase 1: High-Throughput Ingestion
    GameService->>gRPC: DispatchEvent(event_id, tenant_id, event_type, payload)
    gRPC->>PG: INSERT webhook_events (Status: PENDING)
    alt Duplicate Event ID
        gRPC-->>GameService: STATUS_DUPLICATE
    else New Event
        gRPC->>Dispatcher: Enqueue to buffered channel (buf=10,000)
        gRPC-->>GameService: STATUS_ACCEPTED
    end

    %% Worker Execution
    Note over Dispatcher,Target: Phase 2: Concurrent Worker Processing & HMAC Signing
    Dispatcher->>PG: Fetch active endpoints for tenant + event_type
    Dispatcher->>Redis: Check Sliding-Window Rate Limit (1s window)
    alt Rate limit exceeded
        Dispatcher->>Redis: Delay event by 500ms-1500ms without consuming retry count
    else Rate limit OK
        Dispatcher->>Target: POST /webhook (X-Webhook-Signature: HMAC-SHA256, X-Event-ID)
        alt Response 2xx OK
            Dispatcher->>PG: Record attempt & UPDATE webhook_events (Status: DELIVERED)
            Dispatcher->>Dispatcher: Emit Prometheus metric (success)
        else Non-2xx (500, 502, 429) or Network Timeout
            Dispatcher->>PG: Record failed attempt (status, error message, duration)
            alt RetryCount < MaxRetries (e.g., 5)
                Note over Dispatcher,Redis: Phase 3: Exponential Backoff with Jitter
                Dispatcher->>Dispatcher: Compute backoff = initial * 2^attempt + jitter
                Dispatcher->>PG: UPDATE webhook_events (Status: RETRYING, next_retry_at)
                Dispatcher->>Redis: ZADD dispatcher:retry_zset (score: unix_timestamp_ms)
            else RetryCount >= MaxRetries
                Note over Dispatcher,PG: Phase 4: Dead Letter Queue (DLQ)
                Dispatcher->>PG: INSERT dead_letter_queue & UPDATE webhook_events (Status: FAILED)
                Dispatcher->>Dispatcher: Emit Prometheus metric (dlq_enqueued)
            end
        end
    end

    %% Redis Retry Poller
    loop Every 1 second
        Dispatcher->>Redis: Lua atomic ZRANGEBYSCORE + ZREM (score <= now)
        Redis-->>Dispatcher: Due retry jobs
        Dispatcher->>Dispatcher: Re-enqueue to worker channel
    end

    %% DLQ Replay
    Note over Admin,PG: Phase 5: Operator Inspection & Manual Replay
    Admin->>Admin: POST /api/v1/dlq/:id/replay
    Admin->>PG: Reset status to PENDING & mark DLQ replayed
    Admin->>Dispatcher: Re-enqueue to worker pool
```

---

## 📂 Project Directory Structure

```text
├── api/
│   └── proto/dispatcher/v1/
│       ├── dispatcher.proto           # Protobuf v3 contract
│       ├── dispatcher.pb.go           # Generated Go protobuf types
│       └── dispatcher_grpc.pb.go      # Generated gRPC client/server stubs
├── cmd/
│   ├── server/
│   │   └── main.go                    # Microservice entrypoint (gRPC + HTTP + Worker pool)
│   ├── mockreceiver/
│   │   └── main.go                    # Flaky game studio mock receiver for local testing
│   └── demo/
│       └── main.go                    # Automated end-to-end verification client
├── docs/
│   └── screenshots/                   # High-resolution dashboard & metrics screenshots
│       ├── dashboard_overview.png
│       └── metrics_prometheus.png
├── internal/
│   ├── config/
│   │   └── config.go                  # 12-factor environment configuration
│   ├── domain/
│   │   └── models.go                  # WebhookEndpoint, EventMessage, DeliveryAttempt, DLQItem
│   ├── metrics/
│   │   └── metrics.go                 # Prometheus counters, gauges, and latency histograms
│   ├── queue/
│   │   ├── memory/                    # In-memory delay queue fallback
│   │   └── redis/                     # Redis ZSET delay scheduler & sliding-window rate limiter
│   ├── repository/
│   │   ├── memory/                    # In-memory storage fallback for local dev
│   │   └── postgres/                  # pgx/v5 connection pool, transactions, schema migrations
│   ├── security/
│   │   ├── hmac.go                    # HMAC-SHA256 signature generator & constant-time verifier
│   │   └── hmac_test.go               # Security unit tests
│   ├── transport/
│   │   ├── grpc/
│   │   │   └── server.go              # gRPC DispatchEvent implementation with interceptors
│   │   └── http/
│   │       ├── dashboard.html         # Embedded modern light-mode management UI
│   │       └── server.go              # Echo REST endpoints: management, monitoring, DLQ replay
│   └── worker/
│       ├── backoff.go                 # Exponential backoff calculator with full jitter
│       ├── backoff_test.go            # Backoff unit tests
│       ├── pool.go                    # Bounded worker pool, connection-pooled HTTP client
│       └── pool_test.go               # Channel draining and graceful shutdown unit tests
├── migrations/
│   ├── 000001_init_schema.up.sql      # PostgreSQL schema migration
│   ├── 000001_init_schema.down.sql    # Rollback migration
│   └── migrations.go                  # Embedded SQL migration assets
├── docker-compose.yml                 # PostgreSQL, Redis, App, Mock Receiver, Prometheus
├── Dockerfile                         # Production multi-stage Alpine build
├── Makefile                           # Development tasks (proto, build, test, up, demo)
├── prometheus.yml                     # Prometheus scrape configuration
└── README.md
```

---

## ⚡ Quick Start

### Option 1: Run Locally (Standalone Mode)
The service features an automatic in-memory fallback, allowing you to run it directly without installing PostgreSQL or Redis:

```bash
# Terminal 1: Run the Mock Game Studio Receiver
go run ./cmd/mockreceiver -port 9090 -secret mock-secret-key-12345 -fail-rate 0.4

# Terminal 2: Run the Webhook Dispatcher Microservice
go run ./cmd/server
```

Now open **[http://localhost:8080](http://localhost:8080)** in your browser!

### Option 2: Run with Docker Compose
To run the full distributed production stack with PostgreSQL, Redis, Prometheus, and the Go application:

```bash
# 1. Start all containers
docker compose up -d --build

# 2. View application logs
docker compose logs -f app

# 3. In another terminal, run the automated verification demo
go run ./cmd/demo -scenario all

# 4. Stop containers
docker compose down -v
```

### Port Mappings
| Service | Port | Description |
|---|---|---|
| Webhook Dispatcher (HTTP) | `:8080` | Web UI Dashboard, Admin REST API & `/metrics` |
| Webhook Dispatcher (gRPC) | `:50051` | High-throughput ingestion RPC |
| Mock Game Studio Receiver | `:9090` | Flaky webhook receiver simulator |
| Prometheus UI | `:9091` | Metrics dashboard & query browser |
| PostgreSQL | `:5432` | Relational event log & DLQ database |
| Redis | `:6379` | Delay queue & sliding-window rate limiter |

---

## 📡 REST Management API Reference

### 1. Register Webhook Endpoint
```bash
curl -X POST http://localhost:8080/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "studio-epic-games",
    "event_type": "payment.completed",
    "target_url": "http://localhost:9090/webhook",
    "secret_key": "mock-secret-key-12345",
    "rate_limit_per_sec": 100
  }'
```

### 2. Dispatch Live Event via HTTP (Manual / Web Trigger)
```bash
curl -X POST http://localhost:8080/api/v1/dispatch \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "studio-epic-games",
    "event_type": "payment.completed",
    "payload": "{\"payment_id\":\"pay_101\",\"amount\":49.99,\"currency\":\"USD\"}"
  }'
```

### 3. List Registered Endpoints
```bash
curl http://localhost:8080/api/v1/endpoints?tenant_id=studio-epic-games
```

### 4. Inspect Dead Letter Queue (DLQ)
```bash
curl http://localhost:8080/api/v1/dlq
```

### 5. Manually Replay DLQ Item
```bash
curl -X POST http://localhost:8080/api/v1/dlq/evt-web-12345/replay
```

### 6. Health & Readiness Check
```bash
curl http://localhost:8080/healthz
```

---

## 🔒 Webhook HMAC Signature Verification

External game studios should verify incoming HTTP POST requests using the `X-Webhook-Signature` header:

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
)

// VerifyWebhook validates that the signature matches the HMAC-SHA256 digest of payload.
func VerifyWebhook(secret string, payload []byte, signatureHeader string) bool {
	cleanSig := strings.TrimPrefix(signatureHeader, "sha256=")

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	expectedHex := hex.EncodeToString(mac.Sum(nil))

	sigBytes, err1 := hex.DecodeString(cleanSig)
	expBytes, err2 := hex.DecodeString(expectedHex)
	if err1 != nil || err2 != nil {
		return false
	}

	return subtle.ConstantTimeCompare(sigBytes, expBytes) == 1
}
```

---

## 🧪 Unit Testing

Run the full unit test suite covering cryptographic signing, exponential backoff with full jitter and bounds, and worker pool channel draining:

```bash
go test -v ./...
```

To regenerate Protobuf Go stubs from `.proto` definition:
```bash
make proto
```

---

## 📄 License
MIT License. Built with ❤️ in Go for high-concurrency event-driven systems.
