package queue

import (
	"context"

	"github.com/labstack/gommon/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	Consume(handler func(context.Context, []byte))
	Close() error
}

type consumer struct {
	ch         *amqp.Channel
	deliveryCh <-chan amqp.Delivery
}

func NewConsumer(ch *amqp.Channel, config ConsumeConfig) Consumer {
	// Start consuming messages from the queue
	deliveryCh, err := ch.Consume(
		config.Queue,     // queue name
		config.Consumer,  // consumer
		config.AutoAck,   // auto-ack
		config.Exclusive, // exclusive
		config.NoLocal,   // no-local
		config.NoWait,    // no-wait
		config.Args,      // args
	)
	if err != nil {
		log.Error(err)
	}

	return &consumer{ch, deliveryCh}
}

// Consume starts consuming messages from the queue and invokes the handler for each message received.
func (c *consumer) Consume(handler func(context.Context, []byte)) {
	// Create a channel to indefinitely wait for messages
	forever := make(chan bool)
	// Start a goroutine to handle messages received from the queue
	go func() {
		for msg := range c.deliveryCh {
			// Invoke the handler for each message received
			handler(context.Background(), msg.Body)
		}
	}()

	// Log a message indicating that the consumer is waiting for messages
	log.Info("Waiting for messages...")

	// Wait indefinitely for messages
	<-forever
}

// Close closes the consumer by closing the channel.
func (c *consumer) Close() error {
	return c.ch.Close()
}
