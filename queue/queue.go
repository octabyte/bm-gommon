package queue

import (
	"context"
	"fmt"
	"log"
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

// Exchange represents a RabbitMQ exchange with its queues and bindings
type Exchange struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  ExchangeConfig
}

// NewExchange creates a new exchange instance
func NewExchange(url string, config ExchangeConfig) (*Exchange, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}

	exchange := &Exchange{
		conn:    conn,
		channel: ch,
		config:  config,
	}

	// Declare the exchange
	if err := exchange.declare(); err != nil {
		exchange.Close()
		return nil, err
	}

	return exchange, nil
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
		queueConfig.Name, // Using queue name from queueConfig
		bindingConfig.RoutingKey,
		e.config.Name,
		bindingConfig.NoWait,
		bindingConfig.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}

	return nil
}

// Publish sends a message to the exchange
func (e *Exchange) Publish(routingKey string, message []byte) error {
	ctx := context.Background()
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
	msgs, err := e.channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %v", err)
	}

	go func() {
		for msg := range msgs {
			err := handler(msg.Body)
			if err != nil {
				log.Printf("Error processing message: %v", err)
				msg.Nack(false, false) // Don't requeue failed messages
			} else {
				msg.Ack(false)
			}
		}
	}()

	return nil
}

// Close closes the connection and channel
func (e *Exchange) Close() {
	if e.channel != nil {
		e.channel.Close()
	}
	if e.conn != nil {
		e.conn.Close()
	}
}
