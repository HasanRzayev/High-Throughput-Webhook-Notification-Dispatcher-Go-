package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"webhook-dispatcher/internal/config"
	"webhook-dispatcher/internal/domain"
	"webhook-dispatcher/internal/metrics"
	"webhook-dispatcher/internal/queue/redis"
)

// MockRepository implements worker.Repository for unit tests
type mockRepository struct {
	mu           sync.Mutex
	statusMap    map[string]string
	attempts     []*domain.DeliveryAttempt
	endpoints    map[string]*domain.WebhookEndpoint
	dlqItems     []*domain.DLQItem
}

func newMockRepository() *mockRepository {
	return &mockRepository{
		statusMap: make(map[string]string),
		endpoints: make(map[string]*domain.WebhookEndpoint),
	}
}

func (m *mockRepository) FindMatchingEndpoints(ctx context.Context, tenantID, eventType string) ([]*domain.WebhookEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var list []*domain.WebhookEndpoint
	for _, ep := range m.endpoints {
		if ep.TenantID == tenantID {
			list = append(list, ep)
		}
	}
	return list, nil
}

func (m *mockRepository) GetEndpoint(ctx context.Context, id string) (*domain.WebhookEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ep, ok := m.endpoints[id]
	if !ok {
		return nil, nil
	}
	return ep, nil
}

func (m *mockRepository) CreateEvent(ctx context.Context, ev *domain.EventMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

func (m *mockRepository) RecordAttempt(ctx context.Context, att *domain.DeliveryAttempt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts = append(m.attempts, att)
	return nil
}

func (m *mockRepository) UpdateEventStatus(ctx context.Context, eventID string, status string, retryCount int, nextRetryAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusMap[eventID] = status
	return nil
}

func (m *mockRepository) MoveToDLQ(ctx context.Context, dlq *domain.DLQItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlqItems = append(m.dlqItems, dlq)
	return nil
}

// MockScheduler implements worker.RetryScheduler for unit tests
type mockScheduler struct {
	mu         sync.Mutex
	tasks      []*redis.RetryTask
	allowCalls int64
}

func (m *mockScheduler) ScheduleRetry(ctx context.Context, task *redis.RetryTask, executeAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = append(m.tasks, task)
	return nil
}

func (m *mockScheduler) PopDueTasks(ctx context.Context, batchSize int) ([]*redis.RetryTask, error) {
	return nil, nil
}

func (m *mockScheduler) GetQueueSize(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.tasks)), nil
}

func (m *mockScheduler) AllowEndpoint(ctx context.Context, endpointID string, limitPerSec int) (bool, error) {
	atomic.AddInt64(&m.allowCalls, 1)
	return true, nil
}

func TestWorkerPool_GracefulDraining(t *testing.T) {
	var receivedCount int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&receivedCount, 1)
		// Small delay to simulate work
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		WorkerConcurrency:     5,
		QueueBufferSize:       100,
		HTTPConnectTimeout:    2 * time.Second,
		HTTPTotalTimeout:      5 * time.Second,
		HTTPMaxIdleConns:      50,
		HTTPMaxConnsPerHost:   10,
		HTTPIdleConnTimeout:   30 * time.Second,
		MaxRetries:            3,
		InitialRetryInterval:  100 * time.Millisecond,
		MaxRetryInterval:      1 * time.Second,
		BackoffMultiplier:     2.0,
		SchedulerPollInterval: 1 * time.Second,
	}

	mockRepo := newMockRepository()
	mockSched := &mockScheduler{}
	m := metrics.NewMetrics(prometheus.NewRegistry())

	dispatcher := NewDispatcher(cfg, mockRepo, mockSched, m)
	dispatcher.Start()

	testEndpoint := &domain.WebhookEndpoint{
		ID:              "ep-test-1",
		TenantID:        "tenant-game-1",
		EventType:       "player.levelup",
		TargetURL:       ts.URL,
		SecretKey:       "test-secret-key",
		RateLimitPerSec: 1000,
		IsActive:        true,
	}

	const jobCount = 25
	for i := 0; i < jobCount; i++ {
		job := &DispatchJob{
			EventID:    "evt-" + time.Now().Format("150405.000000") + "-" + string(rune('A'+i)),
			TenantID:   "tenant-game-1",
			EventType:  "player.levelup",
			Payload:    json.RawMessage(`{"level": 10}`),
			RetryCount: 0,
			MaxRetries: 3,
			Endpoint:   testEndpoint,
		}
		if err := dispatcher.Enqueue(job); err != nil {
			t.Fatalf("failed to enqueue job %d: %v", i, err)
		}
	}

	// Trigger graceful stop
	dispatcher.Stop(5 * time.Second)

	// Verify all 25 jobs were completely drained and processed
	finalCount := atomic.LoadInt64(&receivedCount)
	if finalCount != jobCount {
		t.Fatalf("expected all %d jobs to be drained, got %d processed", jobCount, finalCount)
	}

	// Verify that enqueuing after stop fails with ErrStopped
	postStopJob := &DispatchJob{
		EventID:  "evt-after-stop",
		TenantID: "tenant-game-1",
	}
	err := dispatcher.Enqueue(postStopJob)
	if err != ErrStopped {
		t.Fatalf("expected ErrStopped after dispatcher is stopped, got %v", err)
	}
}
