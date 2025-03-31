package queue

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueConfig struct {
	Name              string
	Durable           bool
	AutoDelete        bool
	Exclusive         bool
	NoWait            bool
	Args              amqp.Table
	TTL               time.Duration
	MaxLength         int
	DeadLetterEnabled bool
	DeadLetterSuffix  string // e.g., ".dlq"
}

func (r *RabbitMQ) DeclareQueue(cfg QueueConfig) (amqp.Queue, error) {
	if cfg.Args == nil {
		cfg.Args = amqp.Table{}
	}

	if cfg.TTL > 0 {
		cfg.Args["x-message-ttl"] = int32(cfg.TTL.Milliseconds())
	}

	if cfg.MaxLength > 0 {
		cfg.Args["x-max-length"] = cfg.MaxLength
	}

	if cfg.DeadLetterEnabled {
		dlqName := cfg.Name + cfg.DeadLetterSuffix
		cfg.Args["x-dead-letter-exchange"] = ""
		cfg.Args["x-dead-letter-routing-key"] = dlqName

		// Ensure DLQ exists
		_, err := r.channel.QueueDeclare(
			dlqName,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,
		)
		if err != nil {
			return amqp.Queue{}, fmt.Errorf("failed to declare DLQ: %w", err)
		}
	}

	return r.channel.QueueDeclare(
		cfg.Name,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		cfg.NoWait,
		cfg.Args,
	)
}
