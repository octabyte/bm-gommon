package queue

import (
	"context"

	"github.com/labstack/gommon/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

const queueType = "quorum"

type ConsumerWithRetry interface {
	Consume(handler func(context.Context, []byte) error)
	Close() error
}

type consumerWithRetry struct {
	ch         *amqp.Channel
	deliveryCh <-chan amqp.Delivery
	config     ConsumeWithRetryConfig
}

func NewConsumerWithRetry(ch *amqp.Channel, config ConsumeWithRetryConfig) (ConsumerWithRetry, error) {
	_, err := ch.QueueDeclare(
		config.Queue+"-dlq",
		true,
		false,
		false,
		false,
		map[string]interface{}{
			"x-queue-type": queueType,
		},
	)
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		config.Queue+"-retry",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-queue-type":              queueType,
			"x-message-ttl":             config.MessageTTL,
			"x-dead-letter-routing-key": config.Queue,
		},
	)
	if err != nil {
		return nil, err
	}

	deliveryCh, err := ch.Consume(
		config.Queue,
		config.Consumer,
		false,
		config.Exclusive,
		config.NoLocal,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return nil, err
	}

	return &consumerWithRetry{ch, deliveryCh, config}, err
}

func (c *consumerWithRetry) Consume(handler func(context.Context, []byte) error) {
	forever := make(chan bool)

	go func() {
		for msg := range c.deliveryCh {
			ctx := context.Background()
			if err := handler(ctx, msg.Body); err != nil {
				if retryCount := getRetryCount(msg.Headers); retryCount < c.config.MaxRetries {
					c.retryMessage(ctx, msg)
				} else {
					c.moveToDLQ(ctx, msg)
				}
			} else {
				_ = msg.Ack(false)
			}
		}
	}()

	log.Info("Waiting for messages...")

	<-forever
}

func (c *consumerWithRetry) Close() error {
	return c.ch.Close()
}

func (c *consumerWithRetry) moveToDLQ(ctx context.Context, msg amqp.Delivery) {
	err := c.ch.PublishWithContext(ctx,
		"",
		c.config.Queue+"-dlq",
		false,
		false,
		amqp.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
			Headers:     msg.Headers,
		},
	)
	if err != nil {
		log.Errorf("Failed to move message to DLQ: %v", err)
	}

	_ = msg.Ack(false)
}

func (c *consumerWithRetry) retryMessage(ctx context.Context, msg amqp.Delivery) {
	err := c.ch.PublishWithContext(ctx,
		"",
		c.config.Queue+"-retry",
		false,
		false,
		amqp.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
			Headers: amqp.Table{
				"x-retry-count": getRetryCount(msg.Headers) + 1,
			},
		},
	)
	if err != nil {
		log.Errorf("Failed to retry message: %v", err)
	}

	_ = msg.Ack(false)
}

func getRetryCount(headers amqp.Table) int {
	if val, ok := headers["x-retry-count"]; ok {
		switch v := val.(type) {
		case int32:
			return int(v)
		default:
			log.Errorf("header x-retry-count type not supported: %v", v)
		}
	}

	return 0
}
