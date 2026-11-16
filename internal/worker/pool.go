package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"webhook-dispatcher/internal/config"
	"webhook-dispatcher/internal/domain"
	"webhook-dispatcher/internal/metrics"
	"webhook-dispatcher/internal/queue/redis"
	"webhook-dispatcher/internal/security"
)

var (
	ErrQueueFull = errors.New("dispatcher job queue is full")
	ErrStopped   = errors.New("dispatcher is stopped")
)

type DispatchJob struct {
	EventID    string
	TenantID   string
	EventType  string
	Payload    json.RawMessage
	RetryCount int
	MaxRetries int
	Endpoint   *domain.WebhookEndpoint
}

type Repository interface {
	FindMatchingEndpoints(ctx context.Context, tenantID, eventType string) ([]*domain.WebhookEndpoint, error)
	GetEndpoint(ctx context.Context, id string) (*domain.WebhookEndpoint, error)
	CreateEvent(ctx context.Context, ev *domain.EventMessage) error
	RecordAttempt(ctx context.Context, att *domain.DeliveryAttempt) error
	UpdateEventStatus(ctx context.Context, eventID string, status string, retryCount int, nextRetryAt *time.Time) error
	MoveToDLQ(ctx context.Context, dlq *domain.DLQItem) error
}

type RetryScheduler interface {
	ScheduleRetry(ctx context.Context, task *redis.RetryTask, executeAt time.Time) error
	PopDueTasks(ctx context.Context, batchSize int) ([]*redis.RetryTask, error)
	GetQueueSize(ctx context.Context) (int64, error)
	AllowEndpoint(ctx context.Context, endpointID string, limitPerSec int) (bool, error)
}

type Dispatcher struct {
	cfg        *config.Config
	repo       Repository
	scheduler  RetryScheduler
	metrics    *metrics.Metrics
	backoff    *BackoffCalculator
	httpClient *http.Client

	jobChan chan *DispatchJob
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	mu      sync.RWMutex
	stopped bool
}

func NewDispatcher(
	cfg *config.Config,
	repo Repository,
	scheduler RetryScheduler,
	m *metrics.Metrics,
) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())

	// Highly optimized HTTP transport for connection reuse and fast timeouts
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.HTTPConnectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.HTTPMaxIdleConns,
		MaxIdleConnsPerHost:   cfg.HTTPMaxConnsPerHost,
		MaxConnsPerHost:       cfg.HTTPMaxConnsPerHost,
		IdleConnTimeout:       cfg.HTTPIdleConnTimeout,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: cfg.HTTPTotalTimeout,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.HTTPTotalTimeout,
	}

	backoff := NewBackoffCalculator(cfg.InitialRetryInterval, cfg.MaxRetryInterval, cfg.BackoffMultiplier)

	return &Dispatcher{
		cfg:        cfg,
		repo:       repo,
		scheduler:  scheduler,
		metrics:    m,
		backoff:    backoff,
		httpClient: client,
		jobChan:    make(chan *DispatchJob, cfg.QueueBufferSize),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start launches worker pool goroutines and the Redis retry poller.
func (d *Dispatcher) Start() {
	slog.Info("Starting worker pool", "workers", d.cfg.WorkerConcurrency, "buffer_size", d.cfg.QueueBufferSize)

	// Launch worker pool
	for i := 0; i < d.cfg.WorkerConcurrency; i++ {
		d.wg.Add(1)
		go d.workerLoop(i)
	}

	// Launch Redis retry poller
	d.wg.Add(1)
	go d.pollerLoop()

	// Launch metrics updater
	d.wg.Add(1)
	go d.metricsLoop()
}

// Enqueue adds a job to the in-memory buffered channel.
func (d *Dispatcher) Enqueue(job *DispatchJob) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.stopped {
		return ErrStopped
	}

	select {
	case d.jobChan <- job:
		d.metrics.QueueLength.WithLabelValues("worker_channel").Set(float64(len(d.jobChan)))
		return nil
	default:
		d.metrics.QueueLength.WithLabelValues("worker_channel").Set(float64(len(d.jobChan)))
		return ErrQueueFull
	}
}

// Stop gracefully stops workers and the poller, draining the queue up to shutdownTimeout.
func (d *Dispatcher) Stop(shutdownTimeout time.Duration) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	d.mu.Unlock()

	slog.Info("Shutting down worker pool, draining remaining queue jobs...")
	d.cancel() // Signals poller and context-aware workers

	// Close incoming job channel
	close(d.jobChan)

	// Wait for workers to drain pending jobs with timeout
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("All worker goroutines finished cleanly")
	case <-time.After(shutdownTimeout):
		slog.Warn("Worker shutdown timed out, force stopping", "timeout", shutdownTimeout)
	}

	d.httpClient.CloseIdleConnections()
}

func (d *Dispatcher) workerLoop(workerID int) {
	defer d.wg.Done()
	slog.Debug("Worker started", "worker_id", workerID)

	for job := range d.jobChan {
		d.metrics.ActiveWorkers.Inc()
		d.processJob(job)
		d.metrics.ActiveWorkers.Dec()
		d.metrics.QueueLength.WithLabelValues("worker_channel").Set(float64(len(d.jobChan)))
	}

	slog.Debug("Worker stopped", "worker_id", workerID)
}

func (d *Dispatcher) processJob(job *DispatchJob) {
	ctx, cancel := context.WithTimeout(context.Background(), d.cfg.HTTPTotalTimeout+2*time.Second)
	defer cancel()

	// Resolve endpoint if not already populated
	if job.Endpoint == nil {
		endpoints, err := d.repo.FindMatchingEndpoints(ctx, job.TenantID, job.EventType)
		if err != nil {
			slog.Error("Failed to lookup endpoints for event", "event_id", job.EventID, "error", err)
			return
		}

		if len(endpoints) == 0 {
			slog.Info("No matching endpoints found for event, marked delivered", "event_id", job.EventID, "tenant_id", job.TenantID, "event_type", job.EventType)
			_ = d.repo.UpdateEventStatus(ctx, job.EventID, domain.StatusDelivered, job.RetryCount, nil)
			return
		}

		// Dispatch to each endpoint
		for _, ep := range endpoints {
			childJob := &DispatchJob{
				EventID:    job.EventID,
				TenantID:   job.TenantID,
				EventType:  job.EventType,
				Payload:    job.Payload,
				RetryCount: job.RetryCount,
				MaxRetries: job.MaxRetries,
				Endpoint:   ep,
			}
			d.deliverToEndpoint(ctx, childJob)
		}
		return
	}

	d.deliverToEndpoint(ctx, job)
}

func (d *Dispatcher) deliverToEndpoint(ctx context.Context, job *DispatchJob) {
	ep := job.Endpoint

	// Check per-endpoint rate limit
	if ep.RateLimitPerSec > 0 {
		allowed, err := d.scheduler.AllowEndpoint(ctx, ep.ID, ep.RateLimitPerSec)
		if err != nil {
			slog.Warn("Rate limiter check error, bypassing", "endpoint_id", ep.ID, "error", err)
		} else if !allowed {
			slog.Warn("Rate limit exceeded for endpoint, re-scheduling retry", "endpoint_id", ep.ID, "rate_limit", ep.RateLimitPerSec)
			d.metrics.RateLimitedTotal.WithLabelValues(ep.ID).Inc()

			// Reschedule with a short jittered delay (500ms - 1500ms) without consuming max_retries
			retryTask := &redis.RetryTask{
				EventID:    job.EventID,
				TenantID:   job.TenantID,
				EventType:  job.EventType,
				Payload:    job.Payload,
				RetryCount: job.RetryCount,
				MaxRetries: job.MaxRetries,
				EndpointID: ep.ID,
			}
			delay := 500*time.Millisecond + time.Duration(time.Now().UnixNano()%1000)*time.Millisecond
			_ = d.scheduler.ScheduleRetry(ctx, retryTask, time.Now().Add(delay))
			return
		}
	}

	// Prepare HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.TargetURL, bytes.NewReader(job.Payload))
	if err != nil {
		slog.Error("Failed to create HTTP request", "target_url", ep.TargetURL, "error", err)
		d.handleFailure(ctx, job, 0, "", fmt.Sprintf("create request: %v", err))
		return
	}

	// Add signature & tracking headers
	signature := security.SignPayload(ep.SecretKey, job.Payload)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Signature", signature)
	req.Header.Set("X-Event-ID", job.EventID)
	req.Header.Set("X-Tenant-ID", job.TenantID)
	req.Header.Set("X-Event-Type", job.EventType)
	req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	req.Header.Set("User-Agent", "webhook-dispatcher/1.0")

	// Execute HTTP request
	startTime := time.Now()
	resp, reqErr := d.httpClient.Do(req)
	duration := time.Since(startTime)
	durationMs := duration.Milliseconds()

	d.metrics.DeliveryDuration.WithLabelValues(job.TenantID, job.EventType).Observe(duration.Seconds())

	// Handle response
	var statusCode int
	var respBody string
	var failureReason string

	if reqErr != nil {
		failureReason = reqErr.Error()
		statusCode = 0
	} else {
		defer resp.Body.Close()
		statusCode = resp.StatusCode

		// Read response body (capped at 4KB)
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		respBody = string(bodyBytes)

		if statusCode < 200 || statusCode >= 300 {
			failureReason = fmt.Sprintf("non-2xx response code: %d", statusCode)
		}
	}

	// Record delivery attempt in PostgreSQL
	attempt := &domain.DeliveryAttempt{
		ID:                  uuid.NewString(),
		EventID:             job.EventID,
		EndpointID:          ep.ID,
		AttemptNumber:       job.RetryCount + 1,
		ResponseStatusCode:  statusCode,
		ResponseBody:        respBody,
		ErrorMessage:        failureReason,
		ExecutionDurationMs: durationMs,
		AttemptedAt:         time.Now().UTC(),
	}
	if recErr := d.repo.RecordAttempt(ctx, attempt); recErr != nil {
		slog.Error("Failed to record delivery attempt", "event_id", job.EventID, "error", recErr)
	}

	// Success case
	if failureReason == "" {
		slog.Info("Webhook delivered successfully",
			"event_id", job.EventID,
			"endpoint_id", ep.ID,
			"target_url", ep.TargetURL,
			"status_code", statusCode,
			"duration_ms", durationMs,
		)

		_ = d.repo.UpdateEventStatus(ctx, job.EventID, domain.StatusDelivered, job.RetryCount, nil)
		d.metrics.DeliveriesTotal.WithLabelValues(job.TenantID, job.EventType, strconv.Itoa(statusCode), "success").Inc()
		return
	}

	// Failure case: schedule retry or move to DLQ
	d.handleFailure(ctx, job, statusCode, respBody, failureReason)
}

func (d *Dispatcher) handleFailure(ctx context.Context, job *DispatchJob, statusCode int, respBody, failureReason string) {
	d.metrics.DeliveriesTotal.WithLabelValues(job.TenantID, job.EventType, strconv.Itoa(statusCode), "failed").Inc()

	nextRetryCount := job.RetryCount + 1
	maxRetries := job.MaxRetries
	if maxRetries <= 0 {
		maxRetries = d.cfg.MaxRetries
	}

	if nextRetryCount >= maxRetries {
		// Event exceeded max retries: move to DLQ
		slog.Error("Event delivery exhausted all retries, moving to DLQ",
			"event_id", job.EventID,
			"tenant_id", job.TenantID,
			"total_attempts", nextRetryCount,
			"last_status", statusCode,
			"error", failureReason,
		)

		dlq := &domain.DLQItem{
			ID:               uuid.NewString(),
			EventID:          job.EventID,
			TenantID:         job.TenantID,
			EventType:        job.EventType,
			Payload:          job.Payload,
			TotalAttempts:    nextRetryCount,
			LastStatusCode:   statusCode,
			LastErrorMessage: failureReason,
			Replayed:         false,
			CreatedAt:        time.Now().UTC(),
		}

		if err := d.repo.MoveToDLQ(ctx, dlq); err != nil {
			slog.Error("Failed to move event to DLQ", "event_id", job.EventID, "error", err)
		}

		d.metrics.DLQTotal.WithLabelValues(job.TenantID, job.EventType, "enqueued").Inc()
		return
	}

	// Calculate exponential backoff duration with jitter
	backoffDuration := d.backoff.ComputeDuration(nextRetryCount)
	executeAt := time.Now().Add(backoffDuration)

	slog.Warn("Webhook delivery failed, scheduling retry",
		"event_id", job.EventID,
		"retry_count", nextRetryCount,
		"max_retries", maxRetries,
		"backoff_duration", backoffDuration.String(),
		"execute_at", executeAt.Format(time.RFC3339),
		"status_code", statusCode,
		"reason", failureReason,
	)

	// Update PostgreSQL status to RETRYING
	_ = d.repo.UpdateEventStatus(ctx, job.EventID, domain.StatusRetrying, nextRetryCount, &executeAt)

	// Schedule in Redis ZSET
	retryTask := &redis.RetryTask{
		EventID:    job.EventID,
		TenantID:   job.TenantID,
		EventType:  job.EventType,
		Payload:    job.Payload,
		RetryCount: nextRetryCount,
		MaxRetries: maxRetries,
		EndpointID: job.Endpoint.ID,
	}

	if err := d.scheduler.ScheduleRetry(ctx, retryTask, executeAt); err != nil {
		slog.Error("Failed to schedule retry task in Redis", "event_id", job.EventID, "error", err)
	}
}

// pollerLoop continuously queries Redis for retry tasks whose schedule time is <= now.
func (d *Dispatcher) pollerLoop() {
	defer d.wg.Done()
	slog.Info("Redis retry poller started", "poll_interval", d.cfg.SchedulerPollInterval)

	ticker := time.NewTicker(d.cfg.SchedulerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			slog.Info("Redis retry poller stopped")
			return
		case <-ticker.C:
			d.pollDueRetries()
		}
	}
}

func (d *Dispatcher) pollDueRetries() {
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()

	tasks, err := d.scheduler.PopDueTasks(ctx, 100)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("Failed to pop due retry tasks from Redis", "error", err)
		}
		return
	}

	for _, task := range tasks {
		// Lookup endpoint if specified
		var ep *domain.WebhookEndpoint
		if task.EndpointID != "" {
			var epErr error
			ep, epErr = d.repo.GetEndpoint(ctx, task.EndpointID)
			if epErr != nil {
				slog.Warn("Failed to find endpoint for retry task", "endpoint_id", task.EndpointID, "error", epErr)
			}
		}

		job := &DispatchJob{
			EventID:    task.EventID,
			TenantID:   task.TenantID,
			EventType:  task.EventType,
			Payload:    task.Payload,
			RetryCount: task.RetryCount,
			MaxRetries: task.MaxRetries,
			Endpoint:   ep,
		}

		if err := d.Enqueue(job); err != nil {
			slog.Warn("Worker queue full when enqueuing retry task, re-scheduling to Redis with 2s delay", "event_id", task.EventID)
			_ = d.scheduler.ScheduleRetry(ctx, task, time.Now().Add(2*time.Second))
		}
	}
}

// metricsLoop periodically reports queue sizes to Prometheus.
func (d *Dispatcher) metricsLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			if size, err := d.scheduler.GetQueueSize(ctx); err == nil {
				d.metrics.QueueLength.WithLabelValues("redis_retry").Set(float64(size))
			}
			cancel()
		}
	}
}
