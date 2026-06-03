package outbox

import (
	"context"
	"encoding/json"
	"time"
)

const (
	// CollectionName is the default MongoDB collection used for outbox events.
	CollectionName = "outbox_events"
	// TableName is the default PostgreSQL table used for outbox events.
	TableName = "outbox_events"

	// StatusPending indicates an event is ready to be claimed by a worker.
	StatusPending = "pending"
	// StatusProcessing indicates an event is currently claimed by a worker.
	StatusProcessing = "processing"
	// StatusPublished indicates an event has been successfully published.
	StatusPublished = "published"
	// StatusFailed indicates an event exhausted retries and needs intervention.
	StatusFailed = "failed"
)

// Destination represents the broker destination for an outbox event.
type Destination string

const (
	// DestStream routes events to Rabbit Streams.
	DestStream Destination = "stream"
	// DestQueue routes events to RabbitMQ topic exchange/queues.
	DestQueue Destination = "queue"
)

type EnqueueConfig struct {
	Destination Destination
}

// EnqueueOption customizes outbox enqueue behavior.
type EnqueueOption func(*EnqueueConfig)

// WithDestination explicitly defines the broker destination for an event.
func WithDestination(destination Destination) EnqueueOption {
	return func(cfg *EnqueueConfig) {
		cfg.Destination = destination
	}
}

// OutboxEvent is the storage-agnostic outbox event payload used by Publisher.
type OutboxEvent struct {
	ID          string          `json:"id"`
	EventName   string          `json:"event_name"`
	Destination Destination     `json:"destination"`
	Payload     json.RawMessage `json:"payload"`
	Metadata    map[string]any  `json:"metadata,omitempty"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	NextRetryAt time.Time       `json:"next_retry_at"`
	LockedBy    *string         `json:"locked_by,omitempty"`
	LockedUntil *time.Time      `json:"locked_until,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}

// Enqueuer is the contract used by application services to save outbox events.
type Enqueuer interface {
	Enqueue(ctx context.Context, eventName string, payload any, metadata map[string]any, opts ...EnqueueOption) error
}

// EventPublisher is the broker adapter used by the outbox publisher worker.
type EventPublisher interface {
	PublishRaw(ctx context.Context, eventName string, payload json.RawMessage, metadata map[string]any) error
}
