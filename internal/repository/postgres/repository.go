package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-dispatcher/internal/config"
	"webhook-dispatcher/internal/domain"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(ctx context.Context, cfg *config.Config) (*Repository, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}

	poolCfg.MaxConns = cfg.PostgresMaxConns
	poolCfg.MinConns = cfg.PostgresMinConns
	poolCfg.MaxConnIdleTime = cfg.PostgresMaxConnIdle
	poolCfg.MaxConnLifetime = cfg.PostgresMaxConnLife

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

func (r *Repository) Ping(ctx context.Context) error {
	if r.pool == nil {
		return errors.New("pool is closed")
	}
	return r.pool.Ping(ctx)
}

func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

// EnsureSchema runs initial table migrations if needed
func (r *Repository) EnsureSchema(ctx context.Context, schemaSQL string) error {
	_, err := r.pool.Exec(ctx, schemaSQL)
	return err
}

// --- Endpoints ---

func (r *Repository) CreateEndpoint(ctx context.Context, ep *domain.WebhookEndpoint) error {
	now := time.Now().UTC()
	ep.CreatedAt = now
	ep.UpdatedAt = now

	query := `
		INSERT INTO webhook_endpoints (id, tenant_id, event_type, target_url, secret_key, rate_limit_per_sec, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.pool.Exec(ctx, query,
		ep.ID,
		ep.TenantID,
		ep.EventType,
		ep.TargetURL,
		ep.SecretKey,
		ep.RateLimitPerSec,
		ep.IsActive,
		ep.CreatedAt,
		ep.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return fmt.Errorf("insert endpoint: %w", err)
	}
	return nil
}

func (r *Repository) GetEndpoint(ctx context.Context, id string) (*domain.WebhookEndpoint, error) {
	query := `
		SELECT id, tenant_id, event_type, target_url, secret_key, rate_limit_per_sec, is_active, created_at, updated_at
		FROM webhook_endpoints WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, query, id)
	var ep domain.WebhookEndpoint
	err := row.Scan(
		&ep.ID,
		&ep.TenantID,
		&ep.EventType,
		&ep.TargetURL,
		&ep.SecretKey,
		&ep.RateLimitPerSec,
		&ep.IsActive,
		&ep.CreatedAt,
		&ep.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan endpoint: %w", err)
	}
	return &ep, nil
}

func (r *Repository) ListEndpoints(ctx context.Context, tenantID string) ([]*domain.WebhookEndpoint, error) {
	query := `
		SELECT id, tenant_id, event_type, target_url, secret_key, rate_limit_per_sec, is_active, created_at, updated_at
		FROM webhook_endpoints
		WHERE ($1 = '' OR tenant_id = $1)
		ORDER BY created_at DESC
	`
	rows, err := r.pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query endpoints: %w", err)
	}
	defer rows.Close()

	var list []*domain.WebhookEndpoint
	for rows.Next() {
		var ep domain.WebhookEndpoint
		if err := rows.Scan(
			&ep.ID,
			&ep.TenantID,
			&ep.EventType,
			&ep.TargetURL,
			&ep.SecretKey,
			&ep.RateLimitPerSec,
			&ep.IsActive,
			&ep.CreatedAt,
			&ep.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan endpoint row: %w", err)
		}
		list = append(list, &ep)
	}
	return list, nil
}

func (r *Repository) FindMatchingEndpoints(ctx context.Context, tenantID, eventType string) ([]*domain.WebhookEndpoint, error) {
	query := `
		SELECT id, tenant_id, event_type, target_url, secret_key, rate_limit_per_sec, is_active, created_at, updated_at
		FROM webhook_endpoints
		WHERE tenant_id = $1 AND is_active = TRUE AND (event_type = $2 OR event_type = '*')
	`
	rows, err := r.pool.Query(ctx, query, tenantID, eventType)
	if err != nil {
		return nil, fmt.Errorf("query matching endpoints: %w", err)
	}
	defer rows.Close()

	var list []*domain.WebhookEndpoint
	for rows.Next() {
		var ep domain.WebhookEndpoint
		if err := rows.Scan(
			&ep.ID,
			&ep.TenantID,
			&ep.EventType,
			&ep.TargetURL,
			&ep.SecretKey,
			&ep.RateLimitPerSec,
			&ep.IsActive,
			&ep.CreatedAt,
			&ep.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan matching endpoint: %w", err)
		}
		list = append(list, &ep)
	}
	return list, nil
}

// --- Events ---

func (r *Repository) CreateEvent(ctx context.Context, ev *domain.EventMessage) error {
	now := time.Now().UTC()
	ev.CreatedAt = now
	ev.UpdatedAt = now

	query := `
		INSERT INTO webhook_events (event_id, tenant_id, event_type, payload, status, retry_count, max_retries, next_retry_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := r.pool.Exec(ctx, query,
		ev.EventID,
		ev.TenantID,
		ev.EventType,
		ev.Payload,
		ev.Status,
		ev.RetryCount,
		ev.MaxRetries,
		ev.NextRetryAt,
		ev.CreatedAt,
		ev.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

func (r *Repository) GetEvent(ctx context.Context, eventID string) (*domain.EventMessage, error) {
	query := `
		SELECT event_id, tenant_id, event_type, payload, status, retry_count, max_retries, next_retry_at, created_at, updated_at
		FROM webhook_events WHERE event_id = $1
	`
	row := r.pool.QueryRow(ctx, query, eventID)
	var ev domain.EventMessage
	err := row.Scan(
		&ev.EventID,
		&ev.TenantID,
		&ev.EventType,
		&ev.Payload,
		&ev.Status,
		&ev.RetryCount,
		&ev.MaxRetries,
		&ev.NextRetryAt,
		&ev.CreatedAt,
		&ev.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan event: %w", err)
	}
	return &ev, nil
}

func (r *Repository) UpdateEventStatus(ctx context.Context, eventID string, status string, retryCount int, nextRetryAt *time.Time) error {
	query := `
		UPDATE webhook_events
		SET status = $2, retry_count = $3, next_retry_at = $4, updated_at = NOW()
		WHERE event_id = $1
	`
	_, err := r.pool.Exec(ctx, query, eventID, status, retryCount, nextRetryAt)
	if err != nil {
		return fmt.Errorf("update event status: %w", err)
	}
	return nil
}

// --- Delivery Attempts ---

func (r *Repository) RecordAttempt(ctx context.Context, att *domain.DeliveryAttempt) error {
	if att.AttemptedAt.IsZero() {
		att.AttemptedAt = time.Now().UTC()
	}
	query := `
		INSERT INTO delivery_attempts (id, event_id, endpoint_id, attempt_number, response_status_code, response_body, error_message, execution_duration_ms, attempted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.pool.Exec(ctx, query,
		att.ID,
		att.EventID,
		att.EndpointID,
		att.AttemptNumber,
		att.ResponseStatusCode,
		att.ResponseBody,
		att.ErrorMessage,
		att.ExecutionDurationMs,
		att.AttemptedAt,
	)
	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}
	return nil
}

func (r *Repository) GetAttemptsByEvent(ctx context.Context, eventID string) ([]*domain.DeliveryAttempt, error) {
	query := `
		SELECT id, event_id, endpoint_id, attempt_number, response_status_code, response_body, error_message, execution_duration_ms, attempted_at
		FROM delivery_attempts
		WHERE event_id = $1
		ORDER BY attempt_number ASC
	`
	rows, err := r.pool.Query(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("query attempts: %w", err)
	}
	defer rows.Close()

	var attempts []*domain.DeliveryAttempt
	for rows.Next() {
		var a domain.DeliveryAttempt
		if err := rows.Scan(
			&a.ID,
			&a.EventID,
			&a.EndpointID,
			&a.AttemptNumber,
			&a.ResponseStatusCode,
			&a.ResponseBody,
			&a.ErrorMessage,
			&a.ExecutionDurationMs,
			&a.AttemptedAt,
		); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		attempts = append(attempts, &a)
	}
	return attempts, nil
}

// --- Dead Letter Queue (DLQ) ---

func (r *Repository) MoveToDLQ(ctx context.Context, item *domain.DLQItem) error {
	now := time.Now().UTC()
	item.CreatedAt = now

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dlq tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update event status to FAILED
	_, err = tx.Exec(ctx, `UPDATE webhook_events SET status = $2, updated_at = NOW() WHERE event_id = $1`, item.EventID, domain.StatusFailed)
	if err != nil {
		return fmt.Errorf("update event to failed: %w", err)
	}

	// Insert into dead_letter_queue
	insertDLQ := `
		INSERT INTO dead_letter_queue (id, event_id, tenant_id, event_type, payload, total_attempts, last_status_code, last_error_message, replayed, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (event_id) DO UPDATE
		SET total_attempts = EXCLUDED.total_attempts,
		    last_status_code = EXCLUDED.last_status_code,
		    last_error_message = EXCLUDED.last_error_message,
		    replayed = FALSE
	`
	_, err = tx.Exec(ctx, insertDLQ,
		item.ID,
		item.EventID,
		item.TenantID,
		item.EventType,
		item.Payload,
		item.TotalAttempts,
		item.LastStatusCode,
		item.LastErrorMessage,
		false,
		item.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert into dlq: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *Repository) ListDLQ(ctx context.Context, limit, offset int) ([]*domain.DLQItem, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		SELECT id, event_id, tenant_id, event_type, payload, total_attempts, last_status_code, last_error_message, replayed, created_at, replayed_at
		FROM dead_letter_queue
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`
	rows, err := r.pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query dlq: %w", err)
	}
	defer rows.Close()

	var items []*domain.DLQItem
	for rows.Next() {
		var item domain.DLQItem
		if err := rows.Scan(
			&item.ID,
			&item.EventID,
			&item.TenantID,
			&item.EventType,
			&item.Payload,
			&item.TotalAttempts,
			&item.LastStatusCode,
			&item.LastErrorMessage,
			&item.Replayed,
			&item.CreatedAt,
			&item.ReplayedAt,
		); err != nil {
			return nil, fmt.Errorf("scan dlq item: %w", err)
		}
		items = append(items, &item)
	}
	return items, nil
}

func (r *Repository) GetDLQItem(ctx context.Context, id string) (*domain.DLQItem, error) {
	query := `
		SELECT id, event_id, tenant_id, event_type, payload, total_attempts, last_status_code, last_error_message, replayed, created_at, replayed_at
		FROM dead_letter_queue
		WHERE id = $1 OR event_id = $1
	`
	row := r.pool.QueryRow(ctx, query, id)
	var item domain.DLQItem
	err := row.Scan(
		&item.ID,
		&item.EventID,
		&item.TenantID,
		&item.EventType,
		&item.Payload,
		&item.TotalAttempts,
		&item.LastStatusCode,
		&item.LastErrorMessage,
		&item.Replayed,
		&item.CreatedAt,
		&item.ReplayedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan dlq item: %w", err)
	}
	return &item, nil
}

func (r *Repository) MarkDLQReplayed(ctx context.Context, id string) error {
	now := time.Now().UTC()
	query := `
		UPDATE dead_letter_queue
		SET replayed = TRUE, replayed_at = $2
		WHERE id = $1 OR event_id = $1
	`
	_, err := r.pool.Exec(ctx, query, id, now)
	if err != nil {
		return fmt.Errorf("update dlq replayed: %w", err)
	}
	return nil
}
