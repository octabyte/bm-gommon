package queue

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
	Publish(ctx context.Context, body []byte) error
	Close() error
}

type publisher struct {
	ch     *amqp.Channel
	config PublishConfig
}

func NewPublisher(ch *amqp.Channel, config PublishConfig) Publisher {
	return &publisher{ch, config}
}

// Publish publishes a message to the specified exchange and routing key.
func (p *publisher) Publish(ctx context.Context, body []byte) error {
	message := amqp.Publishing{
		ContentType:  p.config.ContentType,  // content type
		Body:         body,                  // message body
		DeliveryMode: p.config.DeliveryMode, // delivery mode
	}

	return p.ch.PublishWithContext(
		ctx,                 // context
		p.config.Exchange,   // exchange
		p.config.RoutingKey, // routing key
		false,               // mandatory
		false,               // immediate
		message,
	)
}

// Close closes the publisher, releasing any resources it holds.
func (p *publisher) Close() error {
	// Close the underlying channel and return any error that might occur.
	return p.ch.Close()
}
