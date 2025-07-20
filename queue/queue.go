package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ExchangeType represents the type of exchange
type ExchangeType string

const (
	Direct  ExchangeType = "direct"
	Fanout  ExchangeType = "fanout"
	Topic   ExchangeType = "topic"
	Headers ExchangeType = "headers"
)

// ExchangeConfig holds the configuration for an exchange
type ExchangeConfig struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

// QueueConfig holds the configuration for a queue
type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// BindingConfig holds the configuration for queue binding
type BindingConfig struct {
	RoutingKey string
	NoWait     bool
	Args       amqp.Table
}

// Consumer holds consumer configuration for restart capability
type Consumer struct {
	QueueName string
	Handler   func([]byte) error
	Active    bool
	StopCh    chan struct{}
}

// Exchange represents a RabbitMQ exchange with its queues and bindings
type Exchange struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  ExchangeConfig
	url     string

	// Connection management
	mutex        sync.RWMutex
	connected    bool
	reconnecting bool
	connClosed   chan *amqp.Error
	chanClosed   chan *amqp.Error

	// Consumer management
	consumers     map[string]*Consumer
	consumerMutex sync.RWMutex

	// Queues to recreate after reconnection
	queues     map[string]QueueConfig
	bindings   map[string]BindingConfig
	queueMutex sync.RWMutex
}

// NewExchange creates a new exchange instance
func NewExchange(url string, config ExchangeConfig) (*Exchange, error) {
	exchange := &Exchange{
		config:    config,
		url:       url,
		consumers: make(map[string]*Consumer),
		queues:    make(map[string]QueueConfig),
		bindings:  make(map[string]BindingConfig),
	}

	if err := exchange.connect(); err != nil {
		return nil, err
	}

	// Start connection monitoring
	go exchange.monitorConnection()

	return exchange, nil
}

// connect establishes connection and declares exchange
func (e *Exchange) connect() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	conn, err := amqp.Dial(e.url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open a channel: %v", err)
	}

	e.conn = conn
	e.channel = ch
	e.connected = true
	e.connClosed = make(chan *amqp.Error, 1)
	e.chanClosed = make(chan *amqp.Error, 1)

	// Listen for connection/channel close events
	e.conn.NotifyClose(e.connClosed)
	e.channel.NotifyClose(e.chanClosed)

	// Declare the exchange
	if err := e.declare(); err != nil {
		e.closeConnections()
		return err
	}

	// Recreate queues and restart consumers after reconnection
	e.recreateQueues()
	e.restartConsumers()

	log.Printf("Connected to RabbitMQ and exchange '%s' declared", e.config.Name)
	return nil
}

// monitorConnection monitors connection health and handles reconnection
func (e *Exchange) monitorConnection() {
	for {
		select {
		case err := <-e.connClosed:
			if err != nil {
				log.Printf("RabbitMQ connection closed: %v", err)
				e.handleDisconnection()
			}
		case err := <-e.chanClosed:
			if err != nil {
				log.Printf("RabbitMQ channel closed: %v", err)
				e.handleDisconnection()
			}
		}
	}
}

// handleDisconnection handles connection loss and triggers reconnection
func (e *Exchange) handleDisconnection() {
	e.mutex.Lock()
	if e.reconnecting {
		e.mutex.Unlock()
		return
	}
	e.connected = false
	e.reconnecting = true
	e.mutex.Unlock()

	// Stop all consumers
	e.stopAllConsumers()

	// Attempt reconnection with exponential backoff
	for attempt := 1; attempt <= 10; attempt++ {
		log.Printf("Attempting to reconnect to RabbitMQ (attempt %d)", attempt)

		if err := e.connect(); err != nil {
			delay := time.Duration(attempt) * time.Second
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			log.Printf("Reconnection failed, retrying in %v: %v", delay, err)
			time.Sleep(delay)
			continue
		}

		e.mutex.Lock()
		e.reconnecting = false
		e.mutex.Unlock()
		log.Printf("Successfully reconnected to RabbitMQ")
		return
	}

	log.Printf("Failed to reconnect after 10 attempts, giving up")
	e.mutex.Lock()
	e.reconnecting = false
	e.mutex.Unlock()
}

// recreateQueues recreates all previously declared queues and bindings
func (e *Exchange) recreateQueues() {
	e.queueMutex.RLock()
	defer e.queueMutex.RUnlock()

	for queueName, queueConfig := range e.queues {
		bindingConfig := e.bindings[queueName]

		// Declare the queue
		_, err := e.channel.QueueDeclare(
			queueConfig.Name,
			queueConfig.Durable,
			queueConfig.AutoDelete,
			queueConfig.Exclusive,
			queueConfig.NoWait,
			queueConfig.Args,
		)
		if err != nil {
			log.Printf("Failed to recreate queue %s: %v", queueName, err)
			continue
		}

		// Bind the queue to the exchange
		err = e.channel.QueueBind(
			queueConfig.Name,
			bindingConfig.RoutingKey,
			e.config.Name,
			bindingConfig.NoWait,
			bindingConfig.Args,
		)
		if err != nil {
			log.Printf("Failed to rebind queue %s: %v", queueName, err)
		}
	}
}

// stopAllConsumers stops all active consumers
func (e *Exchange) stopAllConsumers() {
	e.consumerMutex.Lock()
	defer e.consumerMutex.Unlock()

	for _, consumer := range e.consumers {
		if consumer.Active {
			close(consumer.StopCh)
			consumer.Active = false
		}
		consumer.StopCh = make(chan struct{})
	}
}

// restartConsumers restarts all consumers after reconnection
func (e *Exchange) restartConsumers() {
	e.consumerMutex.RLock()
	defer e.consumerMutex.RUnlock()

	for _, consumer := range e.consumers {
		if !consumer.Active {
			go e.startConsumer(consumer)
		}
	}
}

// waitForConnection waits for connection to be ready
func (e *Exchange) waitForConnection() error {
	for i := 0; i < 50; i++ { // Wait up to 5 seconds
		e.mutex.RLock()
		connected := e.connected && !e.reconnecting
		e.mutex.RUnlock()

		if connected {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("connection not ready after timeout")
}

// closeConnections safely closes connections
func (e *Exchange) closeConnections() {
	if e.channel != nil {
		e.channel.Close()
		e.channel = nil
	}
	if e.conn != nil {
		e.conn.Close()
		e.conn = nil
	}
	e.connected = false
}

// declare declares the exchange
func (e *Exchange) declare() error {
	return e.channel.ExchangeDeclare(
		e.config.Name,
		string(e.config.Type),
		e.config.Durable,
		e.config.AutoDelete,
		e.config.Internal,
		e.config.NoWait,
		e.config.Args,
	)
}

// CreateQueue creates a new queue and binds it to the exchange
func (e *Exchange) CreateQueue(queueConfig QueueConfig, bindingConfig BindingConfig) error {
	if err := e.waitForConnection(); err != nil {
		return fmt.Errorf("connection not ready: %v", err)
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// Declare the queue
	_, err := e.channel.QueueDeclare(
		queueConfig.Name,
		queueConfig.Durable,
		queueConfig.AutoDelete,
		queueConfig.Exclusive,
		queueConfig.NoWait,
		queueConfig.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}

	// Bind the queue to the exchange
	err = e.channel.QueueBind(
		queueConfig.Name,
		bindingConfig.RoutingKey,
		e.config.Name,
		bindingConfig.NoWait,
		bindingConfig.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}

	// Store queue config for recreation after reconnection
	e.queueMutex.Lock()
	e.queues[queueConfig.Name] = queueConfig
	e.bindings[queueConfig.Name] = bindingConfig
	e.queueMutex.Unlock()

	return nil
}

// Publish sends a message to the exchange
func (e *Exchange) Publish(ctx context.Context, routingKey string, message []byte) error {
	if err := e.waitForConnection(); err != nil {
		return fmt.Errorf("connection not ready: %v", err)
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.channel.PublishWithContext(
		ctx,
		e.config.Name, // exchange
		routingKey,    // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)
}

// Consume starts consuming messages from a queue
func (e *Exchange) Consume(queueName string, handler func([]byte) error) error {
	if err := e.waitForConnection(); err != nil {
		return fmt.Errorf("connection not ready: %v", err)
	}

	// Create or update consumer
	e.consumerMutex.Lock()
	consumer := &Consumer{
		QueueName: queueName,
		Handler:   handler,
		Active:    false,
		StopCh:    make(chan struct{}),
	}
	e.consumers[queueName] = consumer
	e.consumerMutex.Unlock()

	// Start the consumer
	go e.startConsumer(consumer)

	return nil
}

// startConsumer starts a consumer goroutine
func (e *Exchange) startConsumer(consumer *Consumer) {
	e.mutex.RLock()
	if !e.connected {
		e.mutex.RUnlock()
		return
	}

	msgs, err := e.channel.Consume(
		consumer.QueueName, // queue
		"",                 // consumer
		false,              // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	e.mutex.RUnlock()

	if err != nil {
		log.Printf("Failed to start consumer for queue %s: %v", consumer.QueueName, err)
		return
	}

	e.consumerMutex.Lock()
	consumer.Active = true
	e.consumerMutex.Unlock()

	log.Printf("Started consumer for queue: %s", consumer.QueueName)

	for {
		select {
		case <-consumer.StopCh:
			log.Printf("Stopping consumer for queue: %s", consumer.QueueName)
			return
		case msg, ok := <-msgs:
			if !ok {
				log.Printf("Consumer channel closed for queue: %s", consumer.QueueName)
				e.consumerMutex.Lock()
				consumer.Active = false
				e.consumerMutex.Unlock()
				return
			}

			err := consumer.Handler(msg.Body)
			if err != nil {
				log.Printf("Error processing message: %v", err)
				msg.Nack(false, false)
			} else {
				msg.Ack(false)
			}
		}
	}
}

// Close closes the connection and channel
func (e *Exchange) Close() {
	e.stopAllConsumers()

	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.closeConnections()
}
