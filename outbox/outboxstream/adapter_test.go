package outboxstream

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/octabyte/bm-gommon/stream"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockProducerPublisher struct {
	mock.Mock
}

func (m *mockProducerPublisher) PublishEvent(ctx context.Context, event stream.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func TestStreamPublisherAdapter_PublishRaw(t *testing.T) {
	ctx := context.Background()
	producer := &mockProducerPublisher{}
	adapter := &StreamPublisherAdapter{producer: producer}
	payload := json.RawMessage(`{"schedule_id":"abc-123"}`)
	metadata := map[string]any{"company_id": 77}

	expectedEvent := stream.Event{
		EventName: "schedule.schedule.freed_up",
		Payload:   payload,
		Metadata:  metadata,
	}

	producer.On("PublishEvent", ctx, expectedEvent).Return(nil).Once()

	err := adapter.PublishRaw(ctx, "schedule.schedule.freed_up", payload, metadata)
	require.NoError(t, err)
	producer.AssertExpectations(t)
}

func TestNewWorkerID(t *testing.T) {
	workerID := NewWorkerID()

	require.NotEmpty(t, workerID)
	require.Contains(t, workerID, "-")
	require.Greater(t, len(workerID), 10)
}
