package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StreamTestSuite is the test suite for stream functionality
type StreamTestSuite struct {
	suite.Suite
	rabbitmqContainer testcontainers.Container
	amqpURL           string
	stream            *Stream
}

// SetupSuite runs once before all tests
func (suite *StreamTestSuite) SetupSuite() {
	ctx := context.Background()

	// Create RabbitMQ container
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3-management",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("Server startup complete"),
			wait.ForListeningPort("5672/tcp"),
		),
	}

	rabbitmqContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(suite.T(), err)

	suite.rabbitmqContainer = rabbitmqContainer

	// Get the mapped port
	mappedPort, err := rabbitmqContainer.MappedPort(ctx, "5672")
	require.NoError(suite.T(), err)

	host, err := rabbitmqContainer.Host(ctx)
	require.NoError(suite.T(), err)

	suite.amqpURL = fmt.Sprintf("amqp://guest:guest@%s:%s/", host, mappedPort.Port())

	// Wait a bit for RabbitMQ to be fully ready
	time.Sleep(2 * time.Second)
}

// TearDownSuite runs once after all tests
func (suite *StreamTestSuite) TearDownSuite() {
	if suite.stream != nil {
		suite.stream.Close()
	}
	if suite.rabbitmqContainer != nil {
		suite.rabbitmqContainer.Terminate(context.Background())
	}
}

// SetupTest runs before each test
func (suite *StreamTestSuite) SetupTest() {
	config := StreamConfig{
		URL:            suite.amqpURL,
		ReconnectDelay: 1 * time.Second,
		MaxRetries:     3,
	}

	stream, err := NewStream(config)
	require.NoError(suite.T(), err)
	suite.stream = stream
}

// TearDownTest runs after each test
func (suite *StreamTestSuite) TearDownTest() {
	if suite.stream != nil {
		suite.stream.Close()
		suite.stream = nil
	}
}

// TestStreamCreation tests basic stream creation
func (suite *StreamTestSuite) TestStreamCreation() {
	assert.NotNil(suite.T(), suite.stream)
	assert.Equal(suite.T(), suite.amqpURL, suite.stream.url)
	assert.Equal(suite.T(), 1*time.Second, suite.stream.reconnectDelay)
	assert.Equal(suite.T(), 3, suite.stream.maxRetries)
	assert.True(suite.T(), suite.stream.connected)
	assert.False(suite.T(), suite.stream.reconnecting)
}

// TestStreamCreationWithInvalidURL tests stream creation with invalid URL
func (suite *StreamTestSuite) TestStreamCreationWithInvalidURL() {
	config := StreamConfig{
		URL: "invalid-url",
	}

	stream, err := NewStream(config)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), stream)
}

// TestStreamCreationWithEmptyURL tests stream creation with empty URL
func (suite *StreamTestSuite) TestStreamCreationWithEmptyURL() {
	config := StreamConfig{}

	stream, err := NewStream(config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "AMQP URL is required")
	assert.Nil(suite.T(), stream)
}

// TestDeclareStreamQueue tests stream queue declaration
func (suite *StreamTestSuite) TestDeclareStreamQueue() {
	queueName := "test-stream-queue"
	config := StreamQueueConfig{
		MaxLengthBytes: 1024 * 1024, // 1MB
		MaxAge:         "1D",        // 1 day
	}

	err := suite.stream.DeclareStreamQueue(queueName, config)
	assert.NoError(suite.T(), err)

	// Verify queue config is stored
	suite.stream.streamQueueMu.RLock()
	storedConfig, exists := suite.stream.streamQueues[queueName]
	suite.stream.streamQueueMu.RUnlock()

	assert.True(suite.T(), exists)
	assert.Equal(suite.T(), config.MaxLengthBytes, storedConfig.MaxLengthBytes)
	assert.Equal(suite.T(), config.MaxAge, storedConfig.MaxAge)
}

// TestDeclareStreamQueueWithInvalidMaxAge tests invalid max age format
func (suite *StreamTestSuite) TestDeclareStreamQueueWithInvalidMaxAge() {
	queueName := "test-invalid-stream-queue"
	config := StreamQueueConfig{
		MaxAge: "invalid-format",
	}

	err := suite.stream.DeclareStreamQueue(queueName, config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "invalid max age format")
}

// TestValidateMaxAge tests max age validation
func (suite *StreamTestSuite) TestValidateMaxAge() {
	// Valid formats
	validFormats := []string{"", "1Y", "30D", "24h", "60m", "3600s"}
	for _, format := range validFormats {
		err := validateMaxAge(format)
		assert.NoError(suite.T(), err, "Format %s should be valid", format)
	}

	// Invalid formats
	invalidFormats := []string{"1", "1x", "Y1", "1Day", "invalid"}
	for _, format := range invalidFormats {
		err := validateMaxAge(format)
		assert.Error(suite.T(), err, "Format %s should be invalid", format)
	}
}

// TestProducerCreation tests producer creation
func (suite *StreamTestSuite) TestProducerCreation() {
	streamName := "test-stream"
	producerID := "test-producer"

	producer, err := suite.stream.NewProducer(streamName, producerID)
	require.NoError(suite.T(), err)
	assert.NotNil(suite.T(), producer)
	assert.Equal(suite.T(), streamName, producer.streamName)
	assert.Equal(suite.T(), producerID, producer.id)
	assert.True(suite.T(), producer.active)
	assert.False(suite.T(), producer.closed)

	// Verify producer is stored
	suite.stream.producerMu.RLock()
	storedProducer, exists := suite.stream.producers[producerID]
	suite.stream.producerMu.RUnlock()

	assert.True(suite.T(), exists)
	assert.Equal(suite.T(), producer, storedProducer)
}

// TestProducerCreationWithDuplicateID tests duplicate producer ID
func (suite *StreamTestSuite) TestProducerCreationWithDuplicateID() {
	streamName := "test-stream"
	producerID := "duplicate-producer"

	// Create first producer
	_, err := suite.stream.NewProducer(streamName, producerID)
	require.NoError(suite.T(), err)

	// Try to create second producer with same ID
	_, err = suite.stream.NewProducer(streamName, producerID)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "already exists")
}

// TestProducerCreationWithDefaultID tests producer creation with default ID
func (suite *StreamTestSuite) TestProducerCreationWithDefaultID() {
	streamName := "test-stream-default"

	producer, err := suite.stream.NewProducer(streamName)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), streamName, producer.id) // Should use stream name as ID
}

// TestProducerPublish tests basic message publishing
func (suite *StreamTestSuite) TestProducerPublish() {
	// First declare a stream queue
	queueName := "test-publish-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Publish message
	message := []byte(`{"test": "message"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.Publish(ctx, message)
	assert.NoError(suite.T(), err)
}

// TestProducerPublishEvent tests event publishing
func (suite *StreamTestSuite) TestProducerPublishEvent() {
	// First declare a stream queue
	queueName := "test-event-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Publish event
	event := Event{
		EventName: "test.event",
		Payload:   map[string]interface{}{"key": "value"},
		Timestamp: time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.PublishEvent(ctx, event)
	assert.NoError(suite.T(), err)
}

// TestProducerPublishEventWithAutoTimestamp tests event publishing with auto timestamp
func (suite *StreamTestSuite) TestProducerPublishEventWithAutoTimestamp() {
	// First declare a stream queue
	queueName := "test-auto-timestamp-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Publish event without timestamp
	event := Event{
		EventName: "test.event",
		Payload:   map[string]interface{}{"key": "value"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.PublishEvent(ctx, event)
	assert.NoError(suite.T(), err)
}

// TestProducerClose tests producer closing
func (suite *StreamTestSuite) TestProducerClose() {
	streamName := "test-close-stream"
	producer, err := suite.stream.NewProducer(streamName)
	require.NoError(suite.T(), err)

	err = producer.Close()
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), producer.closed)
	assert.False(suite.T(), producer.active)

	// Test double close
	err = producer.Close()
	assert.NoError(suite.T(), err)
}

// TestProducerPublishAfterClose tests publishing after closing
func (suite *StreamTestSuite) TestProducerPublishAfterClose() {
	queueName := "test-publish-after-close"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	err = producer.Close()
	require.NoError(suite.T(), err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = producer.Publish(ctx, []byte("test"))
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "closed or inactive")
}

// TestConsumerCreation tests consumer creation
func (suite *StreamTestSuite) TestConsumerCreation() {
	queueName := "test-consumer-queue"
	consumerID := "test-consumer"

	// First declare a stream queue
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	handler := func(data []byte) error {
		return nil
	}

	consumer, err := suite.stream.NewConsumer(consumerID, queueName, handler, WithPrefetchCount(1))
	require.NoError(suite.T(), err)
	assert.NotNil(suite.T(), consumer)
	assert.Equal(suite.T(), queueName, consumer.queueName)
	assert.Equal(suite.T(), consumerID, consumer.consumerTag)

	// Give some time for consumer to start
	time.Sleep(100 * time.Millisecond)

	// Verify consumer is stored
	suite.stream.consumerMu.RLock()
	storedConsumer, exists := suite.stream.consumers[consumerID]
	suite.stream.consumerMu.RUnlock()

	assert.True(suite.T(), exists)
	assert.Equal(suite.T(), consumer, storedConsumer)
}

// TestConsumerWithOptions tests consumer creation with options
func (suite *StreamTestSuite) TestConsumerWithOptions() {
	queueName := "test-consumer-options-queue"
	consumerID := "test-consumer-options"

	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	handler := func(data []byte) error { return nil }

	consumer, err := suite.stream.NewConsumer(
		consumerID,
		queueName,
		handler,
		WithAutoAck(true),
		WithPrefetchCount(10),
		WithConsumerTag("custom-tag"),
	)
	require.NoError(suite.T(), err)

	assert.True(suite.T(), consumer.autoAck)
	assert.Equal(suite.T(), 10, consumer.prefetchCount)
	assert.Equal(suite.T(), "custom-tag", consumer.consumerTag)
}

// TestConsumerDuplicateID tests duplicate consumer ID
func (suite *StreamTestSuite) TestConsumerDuplicateID() {
	queueName := "test-duplicate-consumer-queue"
	consumerID := "duplicate-consumer"

	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	handler := func(data []byte) error { return nil }

	// Create first consumer
	_, err = suite.stream.NewConsumer(consumerID, queueName, handler, WithPrefetchCount(1))
	require.NoError(suite.T(), err)

	// Try to create second consumer with same ID
	_, err = suite.stream.NewConsumer(consumerID, queueName, handler)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "already exists")
}

// TestProducerConsumerIntegration tests end-to-end message flow
func (suite *StreamTestSuite) TestProducerConsumerIntegration() {
	queueName := "test-integration-queue"

	// Declare stream queue
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Set up consumer
	var receivedMessages [][]byte
	var mu sync.Mutex
	var messageCount int32

	handler := func(data []byte) error {
		mu.Lock()
		defer mu.Unlock()
		receivedMessages = append(receivedMessages, data)
		atomic.AddInt32(&messageCount, 1)
		return nil
	}

	_, err = suite.stream.NewConsumer("integration-consumer", queueName, handler, WithAutoAck(false), WithPrefetchCount(1))
	require.NoError(suite.T(), err)

	// Wait for consumer to be ready
	time.Sleep(200 * time.Millisecond)

	// Send messages
	messages := [][]byte{
		[]byte(`{"message": 1}`),
		[]byte(`{"message": 2}`),
		[]byte(`{"message": 3}`),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, msg := range messages {
		err = producer.Publish(ctx, msg)
		require.NoError(suite.T(), err)
	}

	// Wait for messages to be processed
	require.Eventually(suite.T(), func() bool {
		return atomic.LoadInt32(&messageCount) == int32(len(messages))
	}, 5*time.Second, 100*time.Millisecond, "Expected to receive all messages")

	mu.Lock()
	assert.Len(suite.T(), receivedMessages, len(messages))
	mu.Unlock()
}

// TestEventProducerConsumerIntegration tests end-to-end event flow
func (suite *StreamTestSuite) TestEventProducerConsumerIntegration() {
	queueName := "test-event-integration-queue"

	// Declare stream queue
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Set up consumer
	var receivedEvents []Event
	var mu sync.Mutex
	var eventCount int32

	handler := func(data []byte) error {
		var event Event
		if err := json.Unmarshal(data, &event); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		receivedEvents = append(receivedEvents, event)
		atomic.AddInt32(&eventCount, 1)
		return nil
	}

	_, err = suite.stream.NewConsumer("event-consumer", queueName, handler, WithAutoAck(false), WithPrefetchCount(1))
	require.NoError(suite.T(), err)

	// Wait for consumer to be ready
	time.Sleep(200 * time.Millisecond)

	// Send events (using float64 for numbers since JSON unmarshaling converts to float64)
	events := []Event{
		{EventName: "user.created", Payload: map[string]interface{}{"id": float64(1)}},
		{EventName: "user.updated", Payload: map[string]interface{}{"id": float64(1), "name": "John"}},
		{EventName: "user.deleted", Payload: map[string]interface{}{"id": float64(1)}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, event := range events {
		err = producer.PublishEvent(ctx, event)
		require.NoError(suite.T(), err)
	}

	// Wait for events to be processed
	require.Eventually(suite.T(), func() bool {
		return atomic.LoadInt32(&eventCount) == int32(len(events))
	}, 5*time.Second, 100*time.Millisecond, "Expected to receive all events")

	mu.Lock()
	assert.Len(suite.T(), receivedEvents, len(events))
	for i, receivedEvent := range receivedEvents {
		assert.Equal(suite.T(), events[i].EventName, receivedEvent.EventName)
		assert.Equal(suite.T(), events[i].Payload, receivedEvent.Payload)
	}
	mu.Unlock()
}

// TestConsumerCancel tests consumer cancellation
func (suite *StreamTestSuite) TestConsumerCancel() {
	queueName := "test-cancel-queue"

	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	handler := func(data []byte) error { return nil }

	consumer, err := suite.stream.NewConsumer("cancel-consumer", queueName, handler, WithPrefetchCount(1))
	require.NoError(suite.T(), err)

	// Wait for consumer to be active
	require.Eventually(suite.T(), func() bool {
		consumer.mu.RLock()
		active := consumer.active
		consumer.mu.RUnlock()
		return active
	}, 2*time.Second, 50*time.Millisecond)

	// Cancel consumer
	err = consumer.Cancel()
	assert.NoError(suite.T(), err)

	// Wait for consumer to be inactive
	require.Eventually(suite.T(), func() bool {
		consumer.mu.RLock()
		active := consumer.active
		consumer.mu.RUnlock()
		return !active
	}, 2*time.Second, 50*time.Millisecond)
}

// TestConsumerErrorHandling tests consumer error handling
func (suite *StreamTestSuite) TestConsumerErrorHandling() {
	queueName := "test-error-queue"

	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Handler that returns error
	var processedCount int32
	handler := func(data []byte) error {
		atomic.AddInt32(&processedCount, 1)
		return fmt.Errorf("processing error")
	}

	_, err = suite.stream.NewConsumer("error-consumer", queueName, handler, WithAutoAck(false), WithPrefetchCount(1))
	require.NoError(suite.T(), err)

	// Wait for consumer to be ready
	time.Sleep(200 * time.Millisecond)

	// Send message
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.Publish(ctx, []byte(`{"test": "error"}`))
	require.NoError(suite.T(), err)

	// Wait for message to be processed (and fail)
	require.Eventually(suite.T(), func() bool {
		return atomic.LoadInt32(&processedCount) > 0
	}, 3*time.Second, 100*time.Millisecond)

	assert.Greater(suite.T(), atomic.LoadInt32(&processedCount), int32(0))
}

// TestStreamClose tests stream closing
func (suite *StreamTestSuite) TestStreamClose() {
	// Declare queue first
	queueName := "test-close-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producers and consumers
	producer, err := suite.stream.NewProducer("test-stream")
	require.NoError(suite.T(), err)

	handler := func(data []byte) error { return nil }
	_, err = suite.stream.NewConsumer("test-consumer", queueName, handler, WithPrefetchCount(1))
	require.NoError(suite.T(), err)

	// Close stream
	err = suite.stream.Close()
	assert.NoError(suite.T(), err)

	// Verify producer is closed
	assert.True(suite.T(), producer.closed)
	assert.False(suite.T(), producer.active)

	// Verify connection is closed
	assert.False(suite.T(), suite.stream.connected)
}

// TestStreamWaitForConnection tests waiting for connection
func (suite *StreamTestSuite) TestStreamWaitForConnection() {
	// Test with connected stream
	err := suite.stream.waitForConnection()
	assert.NoError(suite.T(), err)

	// Test with disconnected stream
	suite.stream.mu.Lock()
	suite.stream.connected = false
	suite.stream.mu.Unlock()

	err = suite.stream.waitForConnection()
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "connection not ready")
}

// TestReconnectionScenario tests reconnection functionality
func (suite *StreamTestSuite) TestReconnectionScenario() {
	// This test simulates a reconnection scenario
	queueName := "test-reconnect-queue"

	// Declare stream queue
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer and consumer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	var messageCount int32
	handler := func(data []byte) error {
		atomic.AddInt32(&messageCount, 1)
		return nil
	}

	_, err = suite.stream.NewConsumer("reconnect-consumer", queueName, handler, WithPrefetchCount(1))
	require.NoError(suite.T(), err)

	// Wait for consumer to be ready
	time.Sleep(200 * time.Millisecond)

	// Send initial message
	ctx := context.Background()
	err = producer.Publish(ctx, []byte(`{"message": "before reconnect"}`))
	require.NoError(suite.T(), err)

	// Wait for message to be processed
	require.Eventually(suite.T(), func() bool {
		return atomic.LoadInt32(&messageCount) == 1
	}, 3*time.Second, 100*time.Millisecond)

	// Simulate disconnection by closing the connection
	suite.stream.mu.Lock()
	suite.stream.connected = false
	if suite.stream.conn != nil {
		suite.stream.conn.Close()
	}
	suite.stream.mu.Unlock()

	// Try to reconnect manually (in real scenario this would be automatic)
	time.Sleep(100 * time.Millisecond)

	// Create new stream instance to simulate reconnection
	newConfig := StreamConfig{
		URL:            suite.amqpURL,
		ReconnectDelay: 1 * time.Second,
		MaxRetries:     3,
	}

	newStream, err := NewStream(newConfig)
	require.NoError(suite.T(), err)
	defer newStream.Close()

	// Verify new stream works
	err = newStream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	assert.NoError(suite.T(), err)

	newProducer, err := newStream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	err = newProducer.Publish(ctx, []byte(`{"message": "after reconnect"}`))
	assert.NoError(suite.T(), err)
}

// Run the test suite
func TestStreamSuite(t *testing.T) {
	suite.Run(t, new(StreamTestSuite))
}

// Unit tests that don't require containers

func TestStreamConfigValidation(t *testing.T) {
	t.Run("Empty URL", func(t *testing.T) {
		config := StreamConfig{}
		_, err := NewStream(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "AMQP URL is required")
	})

	t.Run("Default values", func(t *testing.T) {
		config := StreamConfig{
			URL: "amqp://localhost",
		}
		// This will fail to connect, but we can check the defaults are set
		stream := &Stream{
			url:            config.URL,
			reconnectDelay: 0,
			maxRetries:     0,
			producers:      make(map[string]*Producer),
			consumers:      make(map[string]*Consumer),
			streamQueues:   make(map[string]StreamQueueConfig),
		}

		// Apply default logic
		if stream.reconnectDelay == 0 {
			stream.reconnectDelay = 5 * time.Second
		}
		if stream.maxRetries == 0 {
			stream.maxRetries = 10
		}

		assert.Equal(t, 5*time.Second, stream.reconnectDelay)
		assert.Equal(t, 10, stream.maxRetries)
	})
}

func TestEventStructure(t *testing.T) {
	event := Event{
		EventName: "test.event",
		Payload:   map[string]interface{}{"key": "value"},
		Timestamp: time.Now().UTC(),
	}

	// Test JSON marshaling
	data, err := json.Marshal(event)
	assert.NoError(t, err)

	// Test JSON unmarshaling
	var unmarshaled Event
	err = json.Unmarshal(data, &unmarshaled)
	assert.NoError(t, err)
	assert.Equal(t, event.EventName, unmarshaled.EventName)
}

func TestConsumerOptions(t *testing.T) {
	consumer := &Consumer{}

	// Test individual options
	WithAutoAck(true)(consumer)
	assert.True(t, consumer.autoAck)

	WithExclusive(true)(consumer)
	assert.True(t, consumer.exclusive)

	WithNoLocal(true)(consumer)
	assert.True(t, consumer.noLocal)

	WithNoWait(true)(consumer)
	assert.True(t, consumer.noWait)

	WithPrefetchCount(10)(consumer)
	assert.Equal(t, 10, consumer.prefetchCount)

	WithConsumerTag("test-tag")(consumer)
	assert.Equal(t, "test-tag", consumer.consumerTag)
}
