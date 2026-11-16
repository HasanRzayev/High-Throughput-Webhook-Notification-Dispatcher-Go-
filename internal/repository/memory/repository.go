package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"webhook-dispatcher/internal/domain"
	"webhook-dispatcher/internal/repository/postgres"
)

type Repository struct {
	mu        sync.RWMutex
	endpoints map[string]*domain.WebhookEndpoint
	events    map[string]*domain.EventMessage
	attempts  map[string][]*domain.DeliveryAttempt
	dlq       map[string]*domain.DLQItem
}

func NewRepository() *Repository {
	return &Repository{
		endpoints: make(map[string]*domain.WebhookEndpoint),
		events:    make(map[string]*domain.EventMessage),
		attempts:  make(map[string][]*domain.DeliveryAttempt),
		dlq:       make(map[string]*domain.DLQItem),
	}
}

func (r *Repository) Ping(ctx context.Context) error {
	return nil
}

func (r *Repository) Close() {}

func (r *Repository) CreateEndpoint(ctx context.Context, ep *domain.WebhookEndpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.endpoints[ep.ID]; exists {
		return postgres.ErrAlreadyExists
	}
	now := time.Now().UTC()
	ep.CreatedAt = now
	ep.UpdatedAt = now
	r.endpoints[ep.ID] = ep
	return nil
}

func (r *Repository) GetEndpoint(ctx context.Context, id string) (*domain.WebhookEndpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ep, exists := r.endpoints[id]
	if !exists {
		return nil, postgres.ErrNotFound
	}
	return ep, nil
}

func (r *Repository) ListEndpoints(ctx context.Context, tenantID string) ([]*domain.WebhookEndpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []*domain.WebhookEndpoint
	for _, ep := range r.endpoints {
		if tenantID == "" || ep.TenantID == tenantID {
			list = append(list, ep)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})
	return list, nil
}

func (r *Repository) FindMatchingEndpoints(ctx context.Context, tenantID, eventType string) ([]*domain.WebhookEndpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []*domain.WebhookEndpoint
	for _, ep := range r.endpoints {
		if ep.TenantID == tenantID && ep.IsActive {
			if ep.EventType == eventType || ep.EventType == "*" {
				list = append(list, ep)
			}
		}
	}
	return list, nil
}

func (r *Repository) CreateEvent(ctx context.Context, ev *domain.EventMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.events[ev.EventID]; exists {
		return postgres.ErrAlreadyExists
	}
	now := time.Now().UTC()
	ev.CreatedAt = now
	ev.UpdatedAt = now
	r.events[ev.EventID] = ev
	return nil
}

func (r *Repository) GetEvent(ctx context.Context, eventID string) (*domain.EventMessage, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ev, exists := r.events[eventID]
	if !exists {
		return nil, postgres.ErrNotFound
	}
	return ev, nil
}

func (r *Repository) UpdateEventStatus(ctx context.Context, eventID string, status string, retryCount int, nextRetryAt *time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ev, exists := r.events[eventID]
	if !exists {
		return postgres.ErrNotFound
	}
	ev.Status = status
	ev.RetryCount = retryCount
	ev.NextRetryAt = nextRetryAt
	ev.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *Repository) RecordAttempt(ctx context.Context, att *domain.DeliveryAttempt) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if att.AttemptedAt.IsZero() {
		att.AttemptedAt = time.Now().UTC()
	}
	r.attempts[att.EventID] = append(r.attempts[att.EventID], att)
	return nil
}

func (r *Repository) GetAttemptsByEvent(ctx context.Context, eventID string) ([]*domain.DeliveryAttempt, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	atts := r.attempts[eventID]
	var res []*domain.DeliveryAttempt
	for _, a := range atts {
		res = append(res, a)
	}
	return res, nil
}

func (r *Repository) MoveToDLQ(ctx context.Context, dlq *domain.DLQItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	dlq.CreatedAt = now

	if ev, ok := r.events[dlq.EventID]; ok {
		ev.Status = domain.StatusFailed
		ev.UpdatedAt = now
	}

	r.dlq[dlq.EventID] = dlq
	return nil
}

func (r *Repository) ListDLQ(ctx context.Context, limit, offset int) ([]*domain.DLQItem, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var list []*domain.DLQItem
	for _, item := range r.dlq {
		list = append(list, item)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})

	if offset >= len(list) {
		return nil, nil
	}
	end := offset + limit
	if end > len(list) {
		end = len(list)
	}

	return list[offset:end], nil
}

func (r *Repository) GetDLQItem(ctx context.Context, id string) (*domain.DLQItem, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, item := range r.dlq {
		if item.ID == id || item.EventID == id {
			return item, nil
		}
	}
	return nil, postgres.ErrNotFound
}

func (r *Repository) MarkDLQReplayed(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	for _, item := range r.dlq {
		if item.ID == id || item.EventID == id {
			item.Replayed = true
			item.ReplayedAt = &now
			return nil
		}
	}
	return postgres.ErrNotFound
}
