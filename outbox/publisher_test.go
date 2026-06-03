package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type mockEventStore struct {
	mock.Mock
}

func (m *mockEventStore) ClaimBatch(
	ctx context.Context,
	workerID string,
	batchSize int,
	lockDuration time.Duration,
) ([]OutboxEvent, error) {
	args := m.Called(ctx, workerID, batchSize, lockDuration)

	events, ok := args.Get(0).([]OutboxEvent)
	if !ok && args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return events, args.Error(1)
}

func (m *mockEventStore) MarkPublished(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *mockEventStore) MarkFailed(ctx context.Context, id string, lastErr string) error {
	args := m.Called(ctx, id, lastErr)
	return args.Error(0)
}

func (m *mockEventStore) RescheduleRetry(
	ctx context.Context,
	id string,
	nextRetry time.Time,
	lastErr string,
) error {
	args := m.Called(ctx, id, nextRetry, lastErr)
	return args.Error(0)
}

type mockEventPublisher struct {
	mock.Mock
}

func (m *mockEventPublisher) PublishRaw(
	ctx context.Context,
	eventName string,
	payload json.RawMessage,
	metadata map[string]any,
) error {
	args := m.Called(ctx, eventName, payload, metadata)
	return args.Error(0)
}

type PublisherSuite struct {
	suite.Suite

	ctx          context.Context
	store        *mockEventStore
	streamBroker *mockEventPublisher
	queueBroker  *mockEventPublisher
	publisher    *Publisher
	now          time.Time
}

func TestPublisherSuite(t *testing.T) {
	suite.Run(t, new(PublisherSuite))
}

func (s *PublisherSuite) SetupTest() {
	s.ctx = context.Background()
	s.store = &mockEventStore{}
	s.streamBroker = &mockEventPublisher{}
	s.queueBroker = &mockEventPublisher{}
	s.now = time.Date(2026, 6, 3, 2, 0, 0, 0, time.UTC)

	s.publisher = NewPublisher(
		s.store,
		map[Destination]EventPublisher{
			DestStream: s.streamBroker,
			DestQueue:  s.queueBroker,
		},
		PublisherOptions{
			BatchSize:    5,
			PollInterval: 10 * time.Millisecond,
			LockDuration: 2 * time.Minute,
			MaxAttempts:  5,
			WorkerID:     "worker-A",
		},
	)
	s.publisher.nowFn = func() time.Time { return s.now }
}

func (s *PublisherSuite) TestNewPublisher_AppliesDefaults() {
	p := NewPublisher(s.store, map[Destination]EventPublisher{}, PublisherOptions{})

	require.Equal(s.T(), 100, p.options.BatchSize)
	require.Equal(s.T(), 2*time.Second, p.options.PollInterval)
	require.Equal(s.T(), 2*time.Minute, p.options.LockDuration)
	require.Equal(s.T(), 10, p.options.MaxAttempts)
}

func (s *PublisherSuite) TestProcessBatch_PublishSuccess_MarksPublished() {
	eventID := "evt-publish-success"
	events := []OutboxEvent{
		{
			ID:          eventID,
			EventName:   "schedule.booking.created",
			Destination: DestStream,
			Payload:     json.RawMessage(`{"booking_id":"abc-123"}`),
			Metadata: map[string]any{
				"company_id": uint64(42),
			},
		},
	}

	s.store.On("ClaimBatch", s.ctx, "worker-A", 5, 2*time.Minute).Return(events, nil).Once()
	s.streamBroker.On("PublishRaw", s.ctx, events[0].EventName, events[0].Payload, events[0].Metadata).Return(nil).Once()
	s.store.On("MarkPublished", s.ctx, eventID).Return(nil).Once()

	processed, err := s.publisher.processBatch(s.ctx)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, processed)

	s.store.AssertExpectations(s.T())
	s.streamBroker.AssertExpectations(s.T())
}

func (s *PublisherSuite) TestProcessBatch_PublishFailure_ReschedulesRetry() {
	eventID := "evt-publish-retry"
	events := []OutboxEvent{
		{
			ID:          eventID,
			EventName:   "schedule.booking.status_updated",
			Destination: DestQueue,
			Payload:     json.RawMessage(`{"booking_id":"abc-123"}`),
			Attempts:    1,
		},
	}

	publishErr := errors.New("broker unavailable")
	expectedNextRetry := s.now.Add(2 * time.Second) // attempts(1) + 1 => 2^1 = 2 seconds

	s.store.On("ClaimBatch", s.ctx, "worker-A", 5, 2*time.Minute).Return(events, nil).Once()
	s.queueBroker.On("PublishRaw", s.ctx, events[0].EventName, events[0].Payload, events[0].Metadata).Return(publishErr).Once()
	s.store.On("RescheduleRetry", s.ctx, eventID, expectedNextRetry, publishErr.Error()).Return(nil).Once()

	processed, err := s.publisher.processBatch(s.ctx)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, processed)

	s.store.AssertExpectations(s.T())
	s.queueBroker.AssertExpectations(s.T())
}

func (s *PublisherSuite) TestProcessBatch_PublishFailure_ExhaustedAttempts_MarksFailed() {
	eventID := "evt-publish-failed"
	events := []OutboxEvent{
		{
			ID:          eventID,
			EventName:   "schedule.booking.cancelled",
			Destination: DestStream,
			Payload:     json.RawMessage(`{"booking_id":"abc-123"}`),
			Attempts:    4, // +1 reaches MaxAttempts(5)
		},
	}

	publishErr := errors.New("broker unavailable")

	s.store.On("ClaimBatch", s.ctx, "worker-A", 5, 2*time.Minute).Return(events, nil).Once()
	s.streamBroker.On("PublishRaw", s.ctx, events[0].EventName, events[0].Payload, events[0].Metadata).Return(publishErr).Once()
	s.store.On("MarkFailed", s.ctx, eventID, publishErr.Error()).Return(nil).Once()

	processed, err := s.publisher.processBatch(s.ctx)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, processed)

	s.store.AssertExpectations(s.T())
	s.streamBroker.AssertExpectations(s.T())
}

func (s *PublisherSuite) TestProcessBatch_UnknownDestination_MarksFailed() {
	eventID := "evt-unknown-destination"
	events := []OutboxEvent{
		{
			ID:          eventID,
			EventName:   "schedule.booking.cancelled",
			Destination: Destination("unknown"),
			Payload:     json.RawMessage(`{"booking_id":"abc-123"}`),
		},
	}

	s.store.On("ClaimBatch", s.ctx, "worker-A", 5, 2*time.Minute).Return(events, nil).Once()
	s.store.On("MarkFailed", s.ctx, eventID, "no broker configured for destination: unknown").Return(nil).Once()

	processed, err := s.publisher.processBatch(s.ctx)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, processed)

	s.store.AssertExpectations(s.T())
	s.streamBroker.AssertNotCalled(s.T(), "PublishRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	s.queueBroker.AssertNotCalled(s.T(), "PublishRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func (s *PublisherSuite) TestProcessBatch_ClaimError_ReturnsWrappedError() {
	claimErr := errors.New("mongo unavailable")
	s.store.On("ClaimBatch", s.ctx, "worker-A", 5, 2*time.Minute).Return([]OutboxEvent(nil), claimErr).Once()

	processed, err := s.publisher.processBatch(s.ctx)
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "claim outbox batch")
	require.Equal(s.T(), 0, processed)

	s.store.AssertExpectations(s.T())
}

func (s *PublisherSuite) TestStart_StopsOnContextCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.store.On("ClaimBatch", mock.Anything, "worker-A", 5, 2*time.Minute).Return([]OutboxEvent{}, nil)

	go func() {
		defer close(done)
		s.publisher.Start(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		s.T().Fatal("publisher did not stop after context cancellation")
	}
}

func (s *PublisherSuite) TestBackoffDuration_CappedAtMaximum() {
	require.Equal(s.T(), time.Second, backoffDuration(1))
	require.Equal(s.T(), 2*time.Second, backoffDuration(2))
	require.Equal(s.T(), 4*time.Second, backoffDuration(3))
	require.Equal(s.T(), maxBackoffDelay, backoffDuration(20))
}
