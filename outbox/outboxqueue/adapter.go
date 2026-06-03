package outboxqueue

import (
	"context"
	"encoding/json"
)

type exchangePublisher interface {
	Publish(ctx context.Context, routingKey string, message []byte) error
}

// QueuePublisherAdapter adapts queue.Exchange to outbox.EventPublisher.
type QueuePublisherAdapter struct {
	exchange exchangePublisher
}

// NewQueuePublisherAdapter creates a queue-backed outbox publisher adapter.
func NewQueuePublisherAdapter(exchange exchangePublisher) *QueuePublisherAdapter {
	return &QueuePublisherAdapter{
		exchange: exchange,
	}
}

// PublishRaw publishes the event payload to the configured exchange.
func (q *QueuePublisherAdapter) PublishRaw(
	ctx context.Context,
	eventName string,
	payload json.RawMessage,
	metadata map[string]any,
) error {
	return q.exchange.Publish(ctx, eventName, []byte(payload))
}
