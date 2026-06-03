package outboxstream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"

	"github.com/octabyte/bm-gommon/stream"
)

type producerPublisher interface {
	PublishEvent(ctx context.Context, event stream.Event) error
}

// StreamPublisherAdapter adapts stream.Producer to outbox.EventPublisher.
type StreamPublisherAdapter struct {
	producer producerPublisher
}

// NewStreamPublisherAdapter creates a stream-backed outbox publisher adapter.
func NewStreamPublisherAdapter(producer *stream.Producer) *StreamPublisherAdapter {
	return &StreamPublisherAdapter{
		producer: producer,
	}
}

// PublishRaw publishes the event payload to the configured stream producer.
func (s *StreamPublisherAdapter) PublishRaw(
	ctx context.Context,
	eventName string,
	payload json.RawMessage,
	metadata map[string]any,
) error {
	return s.producer.PublishEvent(ctx, stream.Event{
		EventName: eventName,
		Payload:   payload,
		Metadata:  metadata,
	})
}

// NewWorkerID creates a random worker identifier for outbox consumers.
func NewWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}

	entropy := make([]byte, 10)
	if _, err := rand.Read(entropy); err != nil {
		return hostname + "-pid-" + strconv.Itoa(os.Getpid())
	}

	return hostname + "-" + hex.EncodeToString(entropy)
}
