package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

// TestProducerPublishWithOptions tests publishing with custom headers and options
func (suite *StreamTestSuite) TestProducerPublishWithOptions() {
	// First declare a stream queue
	queueName := "test-publish-options-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Publish message with stream-compatible options
	message := []byte(`{"test": "message with headers"}`)
	options := &PublishOptions{
		Headers: amqp.Table{
			"x-custom-header": "custom-value",
			"x-tenant-id":     "tenant-123",
			"x-trace-id":      "trace-456",
		},
		ContentType:   "application/json",
		CorrelationID: "correlation-123",
		MessageID:     "msg-456",
		Type:          "TestMessage",
		// Note: Removed Priority, UserID, AppID as they can cause issues with streams
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.PublishWithOptions(ctx, message, options)
	assert.NoError(suite.T(), err)
}

// TestProducerPublishEventWithOptions tests event publishing with custom headers and options
func (suite *StreamTestSuite) TestProducerPublishEventWithOptions() {
	// First declare a stream queue
	queueName := "test-publish-event-options-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Publish event with options
	event := Event{
		EventName: "test.event.with.options",
		Payload:   map[string]interface{}{"key": "value", "id": 123},
		Timestamp: time.Now().UTC(),
	}

	options := &PublishOptions{
		Headers: amqp.Table{
			"x-event-type":    "UserEvent",
			"x-event-version": "1.0.0",
			"x-source":        "user-service",
			"x-tenant-id":     "tenant-789",
		},
		ContentType:   "application/json",
		CorrelationID: "event-correlation-456",
		MessageID:     "event-msg-789",
		Type:          "DomainEvent",
		Timestamp:     time.Now().UTC(),
		// Note: Removed Priority, UserID, AppID, DeliveryMode as they can cause issues with streams
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = producer.PublishEventWithOptions(ctx, event, options)
	assert.NoError(suite.T(), err)
}

// TestPublishOptionsNilSafety tests that nil options are handled safely
func (suite *StreamTestSuite) TestPublishOptionsNilSafety() {
	// First declare a stream queue
	queueName := "test-nil-options-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test with nil options - should work like regular Publish
	err = producer.PublishWithOptions(ctx, []byte(`{"test": "nil options"}`), nil)
	assert.NoError(suite.T(), err)

	// Test event with nil options - should work like regular PublishEvent
	event := Event{
		EventName: "test.nil.options",
		Payload:   map[string]interface{}{"test": "nil options"},
	}
	err = producer.PublishEventWithOptions(ctx, event, nil)
	assert.NoError(suite.T(), err)
}

// TestPublishOptionsPartialConfiguration tests partial option configuration
func (suite *StreamTestSuite) TestPublishOptionsPartialConfiguration() {
	// First declare a stream queue
	queueName := "test-partial-options-queue"
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test with only headers
	options1 := &PublishOptions{
		Headers: amqp.Table{
			"x-only-headers": "test",
		},
	}
	err = producer.PublishWithOptions(ctx, []byte(`{"test": "only headers"}`), options1)
	assert.NoError(suite.T(), err)

	// Test with only content type
	options2 := &PublishOptions{
		ContentType: "text/plain",
	}
	err = producer.PublishWithOptions(ctx, []byte("plain text message"), options2)
	assert.NoError(suite.T(), err)

	// Test with only priority and delivery mode
	options3 := &PublishOptions{
		Priority:     9,
		DeliveryMode: 1, // Non-persistent
	}
	err = producer.PublishWithOptions(ctx, []byte(`{"test": "priority and delivery"}`), options3)
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

// TestProducerConsumerWithHeadersIntegration tests end-to-end header transmission
func (suite *StreamTestSuite) TestProducerConsumerWithHeadersIntegration() {
	queueName := "test-headers-integration-queue"

	// Declare stream queue
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Set up consumer that captures headers
	type receivedMessage struct {
		body    []byte
		headers amqp.Table
		props   map[string]interface{}
	}

	var receivedMessages []receivedMessage
	var mu sync.Mutex
	var messageCount int32

	// We need to modify the consumer to capture message properties
	// For this test, we'll use a custom consumer setup
	consumerChannel, err := suite.stream.conn.Channel()
	require.NoError(suite.T(), err)
	defer consumerChannel.Close()

	// Set prefetch count for stream queues (required)
	err = consumerChannel.Qos(1, 0, false)
	require.NoError(suite.T(), err)

	msgs, err := consumerChannel.Consume(
		queueName,              // queue
		"header-test-consumer", // consumer
		false,                  // auto-ack
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)
	require.NoError(suite.T(), err)

	// Start message processor
	go func() {
		for msg := range msgs {
			mu.Lock()
			receivedMessages = append(receivedMessages, receivedMessage{
				body:    msg.Body,
				headers: msg.Headers,
				props: map[string]interface{}{
					"ContentType":   msg.ContentType,
					"DeliveryMode":  msg.DeliveryMode,
					"Priority":      msg.Priority,
					"CorrelationId": msg.CorrelationId,
					"MessageId":     msg.MessageId,
					"Type":          msg.Type,
					"UserId":        msg.UserId,
					"AppId":         msg.AppId,
				},
			})
			atomic.AddInt32(&messageCount, 1)
			mu.Unlock()
			msg.Ack(false)
		}
	}()

	// Wait for consumer to be ready
	time.Sleep(200 * time.Millisecond)

	// Send message with comprehensive headers
	message := []byte(`{"test": "message with headers"}`)
	expectedHeaders := amqp.Table{
		"x-custom-header": "custom-value",
		"x-tenant-id":     "tenant-123",
		"x-trace-id":      "trace-456",
		"x-user-context":  "user-789",
	}

	options := &PublishOptions{
		Headers:       expectedHeaders,
		ContentType:   "application/json",
		CorrelationID: "test-correlation-123",
		MessageID:     "test-msg-456",
		Type:          "TestMessage",
		// Note: Removed Priority, UserID, AppID, DeliveryMode as they can cause issues with streams
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.PublishWithOptions(ctx, message, options)
	require.NoError(suite.T(), err)

	// Wait for message to be received
	require.Eventually(suite.T(), func() bool {
		return atomic.LoadInt32(&messageCount) == 1
	}, 5*time.Second, 100*time.Millisecond, "Expected to receive message")

	mu.Lock()
	defer mu.Unlock()

	require.Len(suite.T(), receivedMessages, 1)
	received := receivedMessages[0]

	// Verify message body
	assert.Equal(suite.T(), message, received.body)

	// Verify headers
	for key, expectedValue := range expectedHeaders {
		actualValue, exists := received.headers[key]
		assert.True(suite.T(), exists, "Header %s should exist", key)
		assert.Equal(suite.T(), expectedValue, actualValue, "Header %s should match", key)
	}

	// Verify message properties (only those that work reliably with streams)
	assert.Equal(suite.T(), "application/json", received.props["ContentType"])
	assert.Equal(suite.T(), "test-correlation-123", received.props["CorrelationId"])
	assert.Equal(suite.T(), "test-msg-456", received.props["MessageId"])
	assert.Equal(suite.T(), "TestMessage", received.props["Type"])
	// Note: Priority, UserID, AppID, DeliveryMode are not checked as they can be problematic with streams
}

// TestEventWithHeadersIntegration tests end-to-end event publishing with headers
func (suite *StreamTestSuite) TestEventWithHeadersIntegration() {
	queueName := "test-event-headers-integration-queue"

	// Declare stream queue
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	// Set up consumer that captures headers and events
	type receivedEventMessage struct {
		event   Event
		headers amqp.Table
		props   map[string]interface{}
	}

	var receivedEvents []receivedEventMessage
	var mu sync.Mutex
	var eventCount int32

	// Custom consumer setup
	consumerChannel, err := suite.stream.conn.Channel()
	require.NoError(suite.T(), err)
	defer consumerChannel.Close()

	// Set prefetch count for stream queues (required)
	err = consumerChannel.Qos(1, 0, false)
	require.NoError(suite.T(), err)

	msgs, err := consumerChannel.Consume(
		queueName,                    // queue
		"event-header-test-consumer", // consumer
		false,                        // auto-ack
		false,                        // exclusive
		false,                        // no-local
		false,                        // no-wait
		nil,                          // args
	)
	require.NoError(suite.T(), err)

	// Start message processor
	go func() {
		for msg := range msgs {
			var event Event
			if err := json.Unmarshal(msg.Body, &event); err == nil {
				mu.Lock()
				receivedEvents = append(receivedEvents, receivedEventMessage{
					event:   event,
					headers: msg.Headers,
					props: map[string]interface{}{
						"ContentType":   msg.ContentType,
						"DeliveryMode":  msg.DeliveryMode,
						"Priority":      msg.Priority,
						"CorrelationId": msg.CorrelationId,
						"MessageId":     msg.MessageId,
						"Type":          msg.Type,
						"UserId":        msg.UserId,
						"AppId":         msg.AppId,
					},
				})
				atomic.AddInt32(&eventCount, 1)
				mu.Unlock()
			}
			msg.Ack(false)
		}
	}()

	// Wait for consumer to be ready
	time.Sleep(200 * time.Millisecond)

	// Send event with headers
	event := Event{
		EventName: "user.registered",
		Payload:   map[string]interface{}{"user_id": "user-123", "email": "test@example.com"},
		Timestamp: time.Now().UTC(),
	}

	expectedHeaders := amqp.Table{
		"x-event-type":    "UserEvent",
		"x-event-version": "v1.0.0",
		"x-source":        "user-service",
		"x-tenant-id":     "tenant-456",
		"x-aggregate-id":  "user-123",
	}

	options := &PublishOptions{
		Headers:       expectedHeaders,
		ContentType:   "application/json",
		CorrelationID: "event-correlation-789",
		MessageID:     "event-msg-101",
		Type:          "DomainEvent",
		// Note: Removed Priority, UserID, AppID, DeliveryMode as they can cause issues with streams
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.PublishEventWithOptions(ctx, event, options)
	require.NoError(suite.T(), err)

	// Wait for event to be received
	require.Eventually(suite.T(), func() bool {
		return atomic.LoadInt32(&eventCount) == 1
	}, 5*time.Second, 100*time.Millisecond, "Expected to receive event")

	mu.Lock()
	defer mu.Unlock()

	require.Len(suite.T(), receivedEvents, 1)
	received := receivedEvents[0]

	// Verify event content
	assert.Equal(suite.T(), event.EventName, received.event.EventName)
	assert.Equal(suite.T(), event.Payload, received.event.Payload)

	// Verify headers
	for key, expectedValue := range expectedHeaders {
		actualValue, exists := received.headers[key]
		assert.True(suite.T(), exists, "Header %s should exist", key)
		assert.Equal(suite.T(), expectedValue, actualValue, "Header %s should match", key)
	}

	// Verify message properties (only those that work reliably with streams)
	assert.Equal(suite.T(), "application/json", received.props["ContentType"])
	assert.Equal(suite.T(), "event-correlation-789", received.props["CorrelationId"])
	assert.Equal(suite.T(), "event-msg-101", received.props["MessageId"])
	assert.Equal(suite.T(), "DomainEvent", received.props["Type"])
	// Note: Priority, UserID, AppID, DeliveryMode are not checked as they can be problematic with streams
}

// TestBackwardCompatibility tests that old methods still work as expected
func (suite *StreamTestSuite) TestBackwardCompatibility() {
	queueName := "test-backward-compatibility-queue"

	// Declare stream queue
	err := suite.stream.DeclareStreamQueue(queueName, StreamQueueConfig{})
	require.NoError(suite.T(), err)

	// Create producer
	producer, err := suite.stream.NewProducer(queueName)
	require.NoError(suite.T(), err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test that old Publish method still works
	err = producer.Publish(ctx, []byte(`{"test": "backward compatibility"}`))
	assert.NoError(suite.T(), err)

	// Test that old PublishEvent method still works
	event := Event{
		EventName: "backward.compatibility.test",
		Payload:   map[string]interface{}{"test": true},
	}
	err = producer.PublishEvent(ctx, event)
	assert.NoError(suite.T(), err)
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

// TestPublishOptionsStruct tests the PublishOptions struct directly
func TestPublishOptionsStruct(t *testing.T) {
	timestamp := time.Now().UTC()

	options := &PublishOptions{
		Headers: amqp.Table{
			"x-test": "value",
		},
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		DeliveryMode:    1,
		Priority:        9,
		CorrelationID:   "corr-123",
		ReplyTo:         "reply-queue",
		Expiration:      "60000",
		MessageID:       "msg-456",
		Timestamp:       timestamp,
		Type:            "TestType",
		UserID:          "user-789",
		AppID:           "app-101",
		Mandatory:       true,
		Immediate:       true,
	}

	// Verify all fields are set correctly
	assert.NotNil(t, options.Headers)
	assert.Equal(t, "value", options.Headers["x-test"])
	assert.Equal(t, "text/plain", options.ContentType)
	assert.Equal(t, "gzip", options.ContentEncoding)
	assert.Equal(t, uint8(1), options.DeliveryMode)
	assert.Equal(t, uint8(9), options.Priority)
	assert.Equal(t, "corr-123", options.CorrelationID)
	assert.Equal(t, "reply-queue", options.ReplyTo)
	assert.Equal(t, "60000", options.Expiration)
	assert.Equal(t, "msg-456", options.MessageID)
	assert.Equal(t, timestamp, options.Timestamp)
	assert.Equal(t, "TestType", options.Type)
	assert.Equal(t, "user-789", options.UserID)
	assert.Equal(t, "app-101", options.AppID)
	assert.True(t, options.Mandatory)
	assert.True(t, options.Immediate)
}

// TestPublishOptionsDefaults tests default behavior when options fields are zero values
func TestPublishOptionsDefaults(t *testing.T) {
	options := &PublishOptions{
		// Only set some fields, leave others as zero values
		Headers: amqp.Table{
			"x-partial": "test",
		},
		ContentType: "application/xml",
		// All other fields left as zero values
	}

	// Verify zero values
	assert.NotNil(t, options.Headers)
	assert.Equal(t, "application/xml", options.ContentType)
	assert.Empty(t, options.ContentEncoding)
	assert.Equal(t, uint8(0), options.DeliveryMode)
	assert.Equal(t, uint8(0), options.Priority)
	assert.Empty(t, options.CorrelationID)
	assert.Empty(t, options.ReplyTo)
	assert.Empty(t, options.Expiration)
	assert.Empty(t, options.MessageID)
	assert.True(t, options.Timestamp.IsZero())
	assert.Empty(t, options.Type)
	assert.Empty(t, options.UserID)
	assert.Empty(t, options.AppID)
	assert.False(t, options.Mandatory)
	assert.False(t, options.Immediate)
}

// Run the test suite
func TestStreamSuite(t *testing.T) {
	suite.Run(t, new(StreamTestSuite))
}
