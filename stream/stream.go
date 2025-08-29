package stream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/goccy/go-json"
	amqp "github.com/rabbitmq/amqp091-go"
)

// StreamQueueConfig holds configuration for stream queues
type StreamQueueConfig struct {
	MaxLengthBytes int64  // Maximum size of the stream in bytes
	MaxAge         string // Maximum age of messages in the stream (e.g., "30D" for 30 days)
}

// PublishOptions holds message metadata and properties for publishing to streams
type PublishOptions struct {
	Headers         amqp.Table // Message headers
	ContentType     string     // MIME content type
	ContentEncoding string     // Content encoding
	DeliveryMode    uint8      // 1 = non-persistent, 2 = persistent
	Priority        uint8      // Message priority (0-9)
	CorrelationID   string     // Correlation ID
	ReplyTo         string     // Reply-to queue name
	Expiration      string     // Message expiration
	MessageID       string     // Message ID
	Timestamp       time.Time  // Message timestamp
	Type            string     // Message type
	UserID          string     // User ID
	AppID           string     // Application ID
	Mandatory       bool       // Mandatory flag
	Immediate       bool       // Immediate flag
}

// Stream represents the AMQP connection and channel management
type Stream struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	url            string
	reconnectDelay time.Duration
	maxRetries     int

	// Connection management
	mu           sync.RWMutex
	connected    bool
	reconnecting bool
	connClosed   chan *amqp.Error
	chanClosed   chan *amqp.Error

	// Producer and consumer management
	producers  map[string]*Producer
	consumers  map[string]*Consumer
	producerMu sync.RWMutex
	consumerMu sync.RWMutex

	// Stream queues to recreate after reconnection
	streamQueues  map[string]StreamQueueConfig
	streamQueueMu sync.RWMutex
}

// Producer represents a message producer with its own channel
type Producer struct {
	channel       *amqp.Channel
	notifyConfirm chan amqp.Confirmation
	mu            sync.RWMutex
	closed        bool
	streamName    string
	id            string // Optional identifier for logging/monitoring
	active        bool
}

// Consumer represents a message consumer with its own channel
type Consumer struct {
	channel       *amqp.Channel
	queueName     string
	handler       func([]byte) error
	autoAck       bool
	exclusive     bool
	noLocal       bool
	noWait        bool
	args          amqp.Table
	consumerTag   string
	mu            sync.RWMutex
	prefetchCount int
	active        bool
	stopCh        chan struct{}
}

// StreamConfig holds configuration for the Stream
type StreamConfig struct {
	URL            string
	ReconnectDelay time.Duration
	MaxRetries     int
}

// Event represents a message event with a name and payload
type Event struct {
	EventName string      `json:"event_name"`
	Payload   interface{} `json:"payload"`
	Timestamp time.Time   `json:"timestamp"`
}

// NewStream creates a new Stream instance
func NewStream(config StreamConfig) (*Stream, error) {
	if config.URL == "" {
		return nil, errors.New("AMQP URL is required")
	}

	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = 5 * time.Second
	}

	if config.MaxRetries == 0 {
		config.MaxRetries = 10
	}

	stream := &Stream{
		url:            config.URL,
		reconnectDelay: config.ReconnectDelay,
		maxRetries:     config.MaxRetries,
		producers:      make(map[string]*Producer),
		consumers:      make(map[string]*Consumer),
		streamQueues:   make(map[string]StreamQueueConfig),
	}

	if err := stream.connect(); err != nil {
		return nil, err
	}

	// Start connection monitoring
	go stream.monitorConnection()

	return stream, nil
}

// validateMaxAge checks if the max age string is in the correct format
func validateMaxAge(maxAge string) error {
	if maxAge == "" {
		return nil
	}

	// Regex pattern for valid duration format:
	// - Starts with one or more digits
	// - Followed by one of the valid units (Y, M, D, h, m, s)
	pattern := `^\d+[YMhDms]$`
	matched, err := regexp.MatchString(pattern, maxAge)
	if err != nil {
		return fmt.Errorf("failed to validate max age: %w", err)
	}

	if !matched {
		return errors.New("max age must be in format: <number><unit> where unit is one of: Y, M, D, h, m, s")
	}

	return nil
}

// DeclareStreamQueue declares a queue with stream configuration
func (s *Stream) DeclareStreamQueue(queueName string, config StreamQueueConfig) error {
	// Validate max age format
	if err := validateMaxAge(config.MaxAge); err != nil {
		return fmt.Errorf("invalid max age format: %w", err)
	}

	if err := s.waitForConnection(); err != nil {
		return fmt.Errorf("connection not ready: %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	args := make(amqp.Table)
	args["x-queue-type"] = "stream" // Always set stream type

	if config.MaxLengthBytes > 0 {
		args["x-max-length-bytes"] = config.MaxLengthBytes
	}

	if config.MaxAge != "" {
		args["x-max-age"] = config.MaxAge
	}

	_, err := s.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare stream queue: %w", err)
	}

	// Store stream queue config for recreation after reconnection
	s.streamQueueMu.Lock()
	s.streamQueues[queueName] = config
	s.streamQueueMu.Unlock()

	return nil
}

// connect establishes the AMQP connection
func (s *Stream) connect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error

	// Connect to RabbitMQ
	s.conn, err = amqp.Dial(s.url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Create main channel for queue declarations
	s.channel, err = s.conn.Channel()
	if err != nil {
		s.conn.Close()
		return fmt.Errorf("failed to create main channel: %w", err)
	}

	s.connected = true
	s.connClosed = make(chan *amqp.Error, 1)
	s.chanClosed = make(chan *amqp.Error, 1)

	// Listen for connection/channel close events
	s.conn.NotifyClose(s.connClosed)
	s.channel.NotifyClose(s.chanClosed)

	// Recreate stream queues and restart producers/consumers after reconnection
	s.recreateStreamQueues()
	s.recreateProducers()
	s.restartConsumers()

	log.Printf("Connected to RabbitMQ for streams")
	return nil
}

// monitorConnection monitors connection health and handles reconnection
func (s *Stream) monitorConnection() {
	for {
		select {
		case err := <-s.connClosed:
			if err != nil {
				log.Printf("RabbitMQ connection closed: %v", err)
				s.handleDisconnection()
			}
		case err := <-s.chanClosed:
			if err != nil {
				log.Printf("RabbitMQ channel closed: %v", err)
				s.handleDisconnection()
			}
		}
	}
}

// handleDisconnection handles connection loss and triggers reconnection
func (s *Stream) handleDisconnection() {
	s.mu.Lock()
	if s.reconnecting {
		s.mu.Unlock()
		return
	}
	s.connected = false
	s.reconnecting = true
	s.mu.Unlock()

	// Stop all consumers and mark producers as inactive
	s.stopAllConsumers()
	s.deactivateAllProducers()

	// Attempt reconnection with exponential backoff
	for attempt := 1; attempt <= s.maxRetries; attempt++ {
		log.Printf("Attempting to reconnect to RabbitMQ (attempt %d)", attempt)

		if err := s.connect(); err != nil {
			delay := time.Duration(attempt) * s.reconnectDelay
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			log.Printf("Reconnection failed, retrying in %v: %v", delay, err)
			time.Sleep(delay)
			continue
		}

		s.mu.Lock()
		s.reconnecting = false
		s.mu.Unlock()
		log.Printf("Successfully reconnected to RabbitMQ")
		return
	}

	log.Printf("Failed to reconnect after %d attempts, giving up", s.maxRetries)
	s.mu.Lock()
	s.reconnecting = false
	s.mu.Unlock()
}

// recreateStreamQueues recreates all previously declared stream queues
func (s *Stream) recreateStreamQueues() {
	s.streamQueueMu.RLock()
	defer s.streamQueueMu.RUnlock()

	for queueName, config := range s.streamQueues {
		args := make(amqp.Table)
		args["x-queue-type"] = "stream"

		if config.MaxLengthBytes > 0 {
			args["x-max-length-bytes"] = config.MaxLengthBytes
		}

		if config.MaxAge != "" {
			args["x-max-age"] = config.MaxAge
		}

		_, err := s.channel.QueueDeclare(
			queueName,
			true,
			false,
			false,
			false,
			args,
		)
		if err != nil {
			log.Printf("Failed to recreate stream queue %s: %v", queueName, err)
		}
	}
}

// recreateProducers recreates all producer channels
func (s *Stream) recreateProducers() {
	s.producerMu.Lock()
	defer s.producerMu.Unlock()

	for id, producer := range s.producers {
		channel, err := s.conn.Channel()
		if err != nil {
			log.Printf("Failed to recreate producer channel %s: %v", id, err)
			continue
		}

		if err := channel.Confirm(false); err != nil {
			log.Printf("Failed to enable publisher confirms for producer %s: %v", id, err)
			channel.Close()
			continue
		}

		producer.mu.Lock()
		if producer.channel != nil {
			producer.channel.Close()
		}
		producer.channel = channel
		producer.notifyConfirm = channel.NotifyPublish(make(chan amqp.Confirmation, 10))
		producer.active = true
		producer.closed = false
		producer.mu.Unlock()
	}
}

// stopAllConsumers stops all active consumers
func (s *Stream) stopAllConsumers() {
	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()

	for _, consumer := range s.consumers {
		consumer.mu.Lock()
		if consumer.active {
			close(consumer.stopCh)
			consumer.active = false
		}
		consumer.stopCh = make(chan struct{})
		consumer.mu.Unlock()
	}
}

// deactivateAllProducers marks all producers as inactive
func (s *Stream) deactivateAllProducers() {
	s.producerMu.RLock()
	defer s.producerMu.RUnlock()

	for _, producer := range s.producers {
		producer.mu.Lock()
		producer.active = false
		producer.mu.Unlock()
	}
}

// restartConsumers restarts all consumers after reconnection
func (s *Stream) restartConsumers() {
	s.consumerMu.RLock()
	defer s.consumerMu.RUnlock()

	for id, consumer := range s.consumers {
		consumer.mu.Lock()
		if !consumer.active {
			// Create new channel for consumer
			channel, err := s.conn.Channel()
			if err != nil {
				log.Printf("Failed to recreate consumer channel %s: %v", id, err)
				consumer.mu.Unlock()
				continue
			}

			if consumer.channel != nil {
				consumer.channel.Close()
			}
			consumer.channel = channel
			consumer.mu.Unlock()

			go s.startConsumer(consumer)
		} else {
			consumer.mu.Unlock()
		}
	}
}

// waitForConnection waits for connection to be ready
func (s *Stream) waitForConnection() error {
	for i := 0; i < 50; i++ { // Wait up to 5 seconds
		s.mu.RLock()
		connected := s.connected && !s.reconnecting
		s.mu.RUnlock()

		if connected {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("connection not ready after timeout")
}

// closeConnections safely closes connections
func (s *Stream) closeConnections() {
	if s.channel != nil {
		s.channel.Close()
		s.channel = nil
	}
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.connected = false
}

// NewProducer creates a new producer with its own channel
// id is optional and used only for logging/monitoring purposes
func (s *Stream) NewProducer(streamName string, id ...string) (*Producer, error) {
	if err := s.waitForConnection(); err != nil {
		return nil, fmt.Errorf("connection not ready: %w", err)
	}

	s.producerMu.Lock()
	defer s.producerMu.Unlock()

	// Use stream name as default ID if none provided
	producerID := streamName
	if len(id) > 0 {
		producerID = id[0]
	}

	if _, exists := s.producers[producerID]; exists {
		return nil, fmt.Errorf("producer with id %s already exists", producerID)
	}

	channel, err := s.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create producer channel: %w", err)
	}

	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	producer := &Producer{
		channel:       channel,
		notifyConfirm: channel.NotifyPublish(make(chan amqp.Confirmation, 10)),
		streamName:    streamName,
		id:            producerID,
		active:        true,
	}

	s.producers[producerID] = producer
	return producer, nil
}

// Close closes the producer's channel
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			return fmt.Errorf("failed to close producer channel: %w", err)
		}
	}

	p.closed = true
	p.active = false
	return nil
}

// Publish sends a message directly to a stream queue with default options
func (p *Producer) Publish(ctx context.Context, message []byte) error {
	return p.PublishWithOptions(ctx, message, nil)
}

// PublishWithOptions sends a message directly to a stream queue with custom headers and properties
func (p *Producer) PublishWithOptions(ctx context.Context, message []byte, options *PublishOptions) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed || !p.active {
		return errors.New("producer is closed or inactive")
	}

	if p.channel == nil {
		return errors.New("producer channel is not initialized")
	}

	// Set default publishing options
	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        message,
	}

	// Apply custom options if provided
	// Note: RabbitMQ Streams don't support mandatory/immediate flags, so we ignore them
	if options != nil {
		if options.Headers != nil {
			publishing.Headers = options.Headers
		}
		if options.ContentType != "" {
			publishing.ContentType = options.ContentType
		}
		if options.ContentEncoding != "" {
			publishing.ContentEncoding = options.ContentEncoding
		}
		// Note: DeliveryMode is ignored for streams (always persistent)
		if options.Priority != 0 {
			publishing.Priority = options.Priority
		}
		if options.CorrelationID != "" {
			publishing.CorrelationId = options.CorrelationID
		}
		if options.ReplyTo != "" {
			publishing.ReplyTo = options.ReplyTo
		}
		if options.Expiration != "" {
			publishing.Expiration = options.Expiration
		}
		if options.MessageID != "" {
			publishing.MessageId = options.MessageID
		}
		if !options.Timestamp.IsZero() {
			publishing.Timestamp = options.Timestamp
		}
		if options.Type != "" {
			publishing.Type = options.Type
		}
		if options.UserID != "" {
			publishing.UserId = options.UserID
		}
		if options.AppID != "" {
			publishing.AppId = options.AppID
		}
		// Mandatory and Immediate are not supported by streams, so we ignore them
	}

	err := p.channel.PublishWithContext(
		ctx,
		"",           // no exchange needed
		p.streamName, // publish directly to stream queue
		false,        // mandatory - not supported by streams
		false,        // immediate - not supported by streams
		publishing,
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation with timeout
	select {
	case confirm := <-p.notifyConfirm:
		if !confirm.Ack {
			return errors.New("message was not acknowledged")
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("timeout waiting for publisher confirmation")
	}

	return nil
}

// PublishEvent publishes an event directly to a stream queue with default options
func (p *Producer) PublishEvent(ctx context.Context, event Event) error {
	return p.PublishEventWithOptions(ctx, event, nil)
}

// PublishEventWithOptions publishes an event directly to a stream queue with custom headers and properties
func (p *Producer) PublishEventWithOptions(ctx context.Context, event Event, options *PublishOptions) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed || !p.active {
		return errors.New("producer is closed or inactive")
	}

	if p.channel == nil {
		return errors.New("producer channel is not initialized")
	}

	// Set timestamp if not already set
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	// Serialize event to JSON
	message, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Set default publishing options
	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        message,
	}

	// Apply custom options if provided
	// Note: RabbitMQ Streams don't support mandatory/immediate flags, so we ignore them
	if options != nil {
		if options.Headers != nil {
			publishing.Headers = options.Headers
		}
		if options.ContentType != "" {
			publishing.ContentType = options.ContentType
		}
		if options.ContentEncoding != "" {
			publishing.ContentEncoding = options.ContentEncoding
		}
		// Note: DeliveryMode is ignored for streams (always persistent)
		if options.Priority != 0 {
			publishing.Priority = options.Priority
		}
		if options.CorrelationID != "" {
			publishing.CorrelationId = options.CorrelationID
		}
		if options.ReplyTo != "" {
			publishing.ReplyTo = options.ReplyTo
		}
		if options.Expiration != "" {
			publishing.Expiration = options.Expiration
		}
		if options.MessageID != "" {
			publishing.MessageId = options.MessageID
		}
		if !options.Timestamp.IsZero() {
			publishing.Timestamp = options.Timestamp
		}
		if options.Type != "" {
			publishing.Type = options.Type
		}
		if options.UserID != "" {
			publishing.UserId = options.UserID
		}
		if options.AppID != "" {
			publishing.AppId = options.AppID
		}
		// Mandatory and Immediate are not supported by streams, so we ignore them
	}

	err = p.channel.PublishWithContext(
		ctx,
		"",           // no exchange needed
		p.streamName, // publish directly to stream queue
		false,        // mandatory - not supported by streams
		false,        // immediate - not supported by streams
		publishing,
	)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	// Wait for confirmation with timeout
	select {
	case confirm := <-p.notifyConfirm:
		if !confirm.Ack {
			return errors.New("event was not acknowledged")
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("timeout waiting for publisher confirmation")
	}

	return nil
}

// NewConsumer creates a new consumer with its own channel
func (s *Stream) NewConsumer(id, queueName string, handler func([]byte) error, options ...ConsumerOption) (*Consumer, error) {
	if err := s.waitForConnection(); err != nil {
		return nil, fmt.Errorf("connection not ready: %w", err)
	}

	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()

	if _, exists := s.consumers[id]; exists {
		return nil, fmt.Errorf("consumer with id %s already exists", id)
	}

	channel, err := s.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer channel: %w", err)
	}

	consumer := &Consumer{
		channel:     channel,
		queueName:   queueName,
		handler:     handler,
		autoAck:     false,
		exclusive:   false,
		noLocal:     false,
		noWait:      false,
		args:        nil,
		consumerTag: id, // Use the id as the default consumer tag
		active:      false,
		stopCh:      make(chan struct{}),
	}

	for _, option := range options {
		option(consumer)
	}

	s.consumers[id] = consumer

	// Start the consumer
	go s.startConsumer(consumer)

	return consumer, nil
}

// ConsumerOption defines options for configuring a consumer
type ConsumerOption func(*Consumer)

// WithAutoAck sets the auto-acknowledge option
func WithAutoAck(autoAck bool) ConsumerOption {
	return func(c *Consumer) {
		c.autoAck = autoAck
	}
}

// WithExclusive sets the exclusive option
func WithExclusive(exclusive bool) ConsumerOption {
	return func(c *Consumer) {
		c.exclusive = exclusive
	}
}

// WithNoLocal sets the no-local option
func WithNoLocal(noLocal bool) ConsumerOption {
	return func(c *Consumer) {
		c.noLocal = noLocal
	}
}

// WithNoWait sets the no-wait option
func WithNoWait(noWait bool) ConsumerOption {
	return func(c *Consumer) {
		c.noWait = noWait
	}
}

// WithArgs sets additional arguments
func WithArgs(args amqp.Table) ConsumerOption {
	return func(c *Consumer) {
		c.args = args
	}
}

// WithConsumerTag sets the consumer tag
func WithConsumerTag(tag string) ConsumerOption {
	return func(c *Consumer) {
		c.consumerTag = tag
	}
}

// WithPrefetchCount sets the prefetch count for the consumer
func WithPrefetchCount(count int) ConsumerOption {
	return func(c *Consumer) {
		c.prefetchCount = count
	}
}

// startConsumer starts a consumer goroutine
func (s *Stream) startConsumer(consumer *Consumer) {
	s.mu.RLock()
	if !s.connected {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()

	consumer.mu.Lock()

	// Set prefetch count if specified
	if consumer.prefetchCount > 0 {
		if err := consumer.channel.Qos(consumer.prefetchCount, 0, false); err != nil {
			log.Printf("Failed to set prefetch count for consumer %s: %v", consumer.consumerTag, err)
			consumer.mu.Unlock()
			return
		}
	}

	msgs, err := consumer.channel.Consume(
		consumer.queueName,
		consumer.consumerTag,
		consumer.autoAck,
		consumer.exclusive,
		consumer.noLocal,
		consumer.noWait,
		consumer.args,
	)
	if err != nil {
		log.Printf("Failed to start consumer %s: %v", consumer.consumerTag, err)
		consumer.mu.Unlock()
		return
	}

	consumer.active = true
	consumer.mu.Unlock()

	log.Printf("Started consumer for queue: %s", consumer.queueName)

	for {
		select {
		case <-consumer.stopCh:
			log.Printf("Stopping consumer for queue: %s", consumer.queueName)
			consumer.mu.Lock()
			consumer.active = false
			consumer.mu.Unlock()
			return
		case msg, ok := <-msgs:
			if !ok {
				log.Printf("Consumer channel closed for queue: %s", consumer.queueName)
				consumer.mu.Lock()
				consumer.active = false
				consumer.mu.Unlock()
				return
			}

			err := consumer.handler(msg.Body)
			if err != nil {
				log.Printf("Error processing message: %v", err)
				if !consumer.autoAck {
					msg.Nack(false, false)
				}
			} else {
				if !consumer.autoAck {
					msg.Ack(false)
				}
			}
		}
	}
}

// Start begins consuming messages (deprecated - now automatically started in NewConsumer)
func (c *Consumer) Start(ctx context.Context) error {
	// This method is now deprecated as consumers are automatically started
	return nil
}

// Cancel stops the consumer
func (c *Consumer) Cancel() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return nil
	}

	if c.channel == nil {
		return errors.New("consumer channel is not initialized")
	}

	if err := c.channel.Cancel(c.consumerTag, false); err != nil {
		return fmt.Errorf("failed to cancel consumer: %w", err)
	}

	close(c.stopCh)
	c.active = false
	return nil
}

// Close closes the AMQP connection and all channels
func (s *Stream) Close() error {
	// Stop all consumers
	s.stopAllConsumers()

	s.producerMu.Lock()
	// Close all producers
	for id, producer := range s.producers {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer %s: %v", id, err)
		}
	}
	s.producerMu.Unlock()

	s.consumerMu.Lock()
	// Close all consumer channels
	for id, consumer := range s.consumers {
		consumer.mu.Lock()
		if consumer.channel != nil {
			if err := consumer.channel.Close(); err != nil {
				log.Printf("Failed to close consumer channel %s: %v", id, err)
			}
		}
		consumer.mu.Unlock()
	}
	s.consumerMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Close the connection
	s.closeConnections()

	return nil
}
