package outboxqueue

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockExchangePublisher struct {
	mock.Mock
}

func (m *mockExchangePublisher) Publish(ctx context.Context, routingKey string, message []byte) error {
	args := m.Called(ctx, routingKey, message)
	return args.Error(0)
}

func TestQueuePublisherAdapter_PublishRaw(t *testing.T) {
	ctx := context.Background()
	exchange := &mockExchangePublisher{}
	adapter := &QueuePublisherAdapter{exchange: exchange}
	payload := json.RawMessage(`{"schedule_id":"abc-123"}`)

	exchange.On("Publish", ctx, "schedule.schedule.freed_up", []byte(payload)).Return(nil).Once()

	err := adapter.PublishRaw(ctx, "schedule.schedule.freed_up", payload, map[string]any{"ignored": true})
	require.NoError(t, err)
	exchange.AssertExpectations(t)
}
