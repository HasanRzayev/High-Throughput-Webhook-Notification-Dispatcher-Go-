package domain

import (
	"encoding/json"
	"time"
)

// Event status constants.
const (
	StatusPending    = "PENDING"
	StatusProcessing = "PROCESSING"
	StatusDelivered  = "DELIVERED"
	StatusRetrying   = "RETRYING"
	StatusFailed     = "FAILED"
)

// WebhookEndpoint represents a destination endpoint registered by a game studio / tenant.
type WebhookEndpoint struct {
	ID              string    `json:"id"`
	TenantID        string    `json:"tenant_id"`
	EventType       string    `json:"event_type"` // e.g. "payment.completed", "game.loot_drop", or "*"
	TargetURL       string    `json:"target_url"`
	SecretKey       string    `json:"secret_key,omitempty"` // Used for HMAC-SHA256 signature
	RateLimitPerSec int       `json:"rate_limit_per_sec"`   // Token bucket or fixed window limit
	IsActive        bool      `json:"is_active"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// EventMessage represents a webhook event payload to be delivered.
type EventMessage struct {
	EventID     string          `json:"event_id"`
	TenantID    string          `json:"tenant_id"`
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	RetryCount  int             `json:"retry_count"`
	MaxRetries  int             `json:"max_retries"`
	NextRetryAt *time.Time      `json:"next_retry_at,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// DeliveryAttempt records each HTTP delivery execution and its outcome.
type DeliveryAttempt struct {
	ID                  string    `json:"id"`
	EventID             string    `json:"event_id"`
	EndpointID          string    `json:"endpoint_id"`
	AttemptNumber       int       `json:"attempt_number"`
	ResponseStatusCode  int       `json:"response_status_code"`
	ResponseBody        string    `json:"response_body,omitempty"`
	ErrorMessage        string    `json:"error_message,omitempty"`
	ExecutionDurationMs int64     `json:"execution_duration_ms"`
	AttemptedAt         time.Time `json:"attempted_at"`
}

// DLQItem represents an event that permanently failed after reaching max retry attempts.
type DLQItem struct {
	ID               string          `json:"id"`
	EventID          string          `json:"event_id"`
	TenantID         string          `json:"tenant_id"`
	EventType        string          `json:"event_type"`
	Payload          json.RawMessage `json:"payload"`
	TotalAttempts    int             `json:"total_attempts"`
	LastStatusCode   int             `json:"last_status_code"`
	LastErrorMessage string          `json:"last_error_message,omitempty"`
	Replayed         bool            `json:"replayed"`
	CreatedAt        time.Time       `json:"created_at"`
	ReplayedAt       *time.Time      `json:"replayed_at,omitempty"`
}
