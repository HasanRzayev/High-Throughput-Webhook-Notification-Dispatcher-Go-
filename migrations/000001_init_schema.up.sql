-- 000001_init_schema.up.sql

CREATE TABLE IF NOT EXISTS webhook_endpoints (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    target_url VARCHAR(1024) NOT NULL,
    secret_key VARCHAR(256) NOT NULL,
    rate_limit_per_sec INT NOT NULL DEFAULT 100,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_endpoints_tenant_event ON webhook_endpoints (tenant_id, event_type) WHERE is_active = TRUE;

CREATE TABLE IF NOT EXISTS webhook_events (
    event_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_status ON webhook_events (status);
CREATE INDEX IF NOT EXISTS idx_events_tenant ON webhook_events (tenant_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id VARCHAR(64) PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL REFERENCES webhook_events(event_id) ON DELETE CASCADE,
    endpoint_id VARCHAR(64) NOT NULL,
    attempt_number INT NOT NULL,
    response_status_code INT NOT NULL DEFAULT 0,
    response_body TEXT,
    error_message TEXT,
    execution_duration_ms BIGINT NOT NULL DEFAULT 0,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_attempts_event_id ON delivery_attempts (event_id);
CREATE INDEX IF NOT EXISTS idx_attempts_attempted_at ON delivery_attempts (attempted_at DESC);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id VARCHAR(64) PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    total_attempts INT NOT NULL,
    last_status_code INT NOT NULL DEFAULT 0,
    last_error_message TEXT,
    replayed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dlq_tenant_id ON dead_letter_queue (tenant_id);
CREATE INDEX IF NOT EXISTS idx_dlq_replayed ON dead_letter_queue (replayed);
