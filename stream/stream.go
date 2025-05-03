package stream

import (
	"context"
	"errors"
	"fmt"
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

// Stream represents the AMQP connection and channel management
type Stream struct {
	conn           *amqp.Connection
	url            string
	reconnectDelay time.Duration
	maxRetries     int
	mu             sync.RWMutex
	notifyClose    chan *amqp.Error
	producers      map[string]*Producer
	consumers      map[string]*Consumer
}

// Producer represents a message producer with its own channel
type Producer struct {
	channel       *amqp.Channel
	notifyConfirm chan amqp.Confirmation
	mu            sync.RWMutex
	closed        bool
	streamName    string
	id            string // Optional identifier for logging/monitoring
}

// Consumer represents a message consumer with its own channel
type Consumer struct {
	channel     *amqp.Channel
	queueName   string
	handler     func([]byte) error
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	args        amqp.Table
	consumerTag string
	mu          sync.RWMutex
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
		config.MaxRetries = 3
	}

	stream := &Stream{
		url:            config.URL,
		reconnectDelay: config.ReconnectDelay,
		maxRetries:     config.MaxRetries,
		notifyClose:    make(chan *amqp.Error),
		producers:      make(map[string]*Producer),
		consumers:      make(map[string]*Consumer),
	}

	if err := stream.connect(); err != nil {
		return nil, err
	}

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

	channel, err := s.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel for queue declaration: %w", err)
	}
	defer channel.Close()

	args := make(amqp.Table)
	args["x-queue-type"] = "stream" // Always set stream type

	if config.MaxLengthBytes > 0 {
		args["x-max-length-bytes"] = config.MaxLengthBytes
	}

	if config.MaxAge != "" {
		args["x-max-age"] = config.MaxAge
	}

	_, err = channel.QueueDeclare(
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

	return nil
}

// connect establishes the AMQP connection
func (s *Stream) connect() error {
	var err error

	// Connect to RabbitMQ
	s.conn, err = amqp.Dial(s.url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	s.notifyClose = s.conn.NotifyClose(make(chan *amqp.Error))

	// Start reconnection handler
	go s.handleReconnect()

	return nil
}

// handleReconnect manages reconnection logic
func (s *Stream) handleReconnect() {
	for err := range s.notifyClose {
		if err != nil {
			fmt.Printf("Connection closed: %v\n", err)
			retries := 0
			for retries < s.maxRetries {
				if err := s.reconnect(); err != nil {
					retries++
					time.Sleep(s.reconnectDelay)
					continue
				}
				break
			}
		}
	}
}

// reconnect re-establishes the connection and recreates all channels
func (s *Stream) reconnect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.connect(); err != nil {
		return err
	}

	// Recreate all producer channels
	for id, producer := range s.producers {
		channel, err := s.conn.Channel()
		if err != nil {
			return fmt.Errorf("failed to recreate producer channel %s: %w", id, err)
		}
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("failed to enable publisher confirms for producer %s: %w", id, err)
		}
		producer.channel = channel
		producer.notifyConfirm = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	// Recreate all consumer channels
	for id, consumer := range s.consumers {
		channel, err := s.conn.Channel()
		if err != nil {
			return fmt.Errorf("failed to recreate consumer channel %s: %w", id, err)
		}
		consumer.channel = channel
		if err := consumer.Start(context.Background()); err != nil {
			return fmt.Errorf("failed to restart consumer %s: %w", id, err)
		}
	}

	return nil
}

// NewProducer creates a new producer with its own channel
// id is optional and used only for logging/monitoring purposes
func (s *Stream) NewProducer(streamName string, id ...string) (*Producer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
		notifyConfirm: channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		streamName:    streamName,
		id:            producerID,
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
	return nil
}

// Publish sends a message directly to a stream queue
func (p *Producer) Publish(ctx context.Context, message []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return errors.New("producer is closed")
	}

	if p.channel == nil {
		return errors.New("producer channel is not initialized")
	}

	err := p.channel.PublishWithContext(
		ctx,
		"",           // no exchange needed
		p.streamName, // publish directly to stream queue
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation
	select {
	case confirm := <-p.notifyConfirm:
		if !confirm.Ack {
			return errors.New("message was not acknowledged")
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// PublishEvent publishes an event directly to a stream queue
func (p *Producer) PublishEvent(ctx context.Context, event Event) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return errors.New("producer is closed")
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

	err = p.channel.PublishWithContext(
		ctx,
		"",           // no exchange needed
		p.streamName, // publish directly to stream queue
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	// Wait for confirmation
	select {
	case confirm := <-p.notifyConfirm:
		if !confirm.Ack {
			return errors.New("event was not acknowledged")
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// NewConsumer creates a new consumer with its own channel
func (s *Stream) NewConsumer(id, queueName string, handler func([]byte) error, options ...ConsumerOption) (*Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
	}

	for _, option := range options {
		option(consumer)
	}

	s.consumers[id] = consumer
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

// Start begins consuming messages
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil {
		return errors.New("consumer channel is not initialized")
	}

	msgs, err := c.channel.Consume(
		c.queueName,
		c.consumerTag, // Use the configured consumer tag
		c.autoAck,
		c.exclusive,
		c.noLocal,
		c.noWait,
		c.args,
	)
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	go func() {
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				if err := c.handler(msg.Body); err != nil {
					if !c.autoAck {
						msg.Nack(false, true)
					}
					continue
				}
				if !c.autoAck {
					msg.Ack(false)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Cancel stops the consumer
func (c *Consumer) Cancel() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil {
		return errors.New("consumer channel is not initialized")
	}

	if err := c.channel.Cancel(c.consumerTag, false); err != nil {
		return fmt.Errorf("failed to cancel consumer: %w", err)
	}

	return nil
}

// Close closes the AMQP connection and all channels
func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all producers
	for id, producer := range s.producers {
		if err := producer.Close(); err != nil {
			return fmt.Errorf("failed to close producer %s: %w", id, err)
		}
	}

	// Close all consumer channels
	for id, consumer := range s.consumers {
		if consumer.channel != nil {
			if err := consumer.channel.Close(); err != nil {
				return fmt.Errorf("failed to close consumer channel %s: %w", id, err)
			}
		}
	}

	// Close the connection
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}

	return nil
}
