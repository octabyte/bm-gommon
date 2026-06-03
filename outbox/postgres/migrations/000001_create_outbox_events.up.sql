CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    event_name VARCHAR(255) NOT NULL,
    destination VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB,
    status VARCHAR(32) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL,
    locked_by TEXT NULL,
    locked_until TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_next_retry
    ON outbox_events (status, next_retry_at);
