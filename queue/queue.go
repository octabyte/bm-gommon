package queue

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

// NewConnection creates a new AMQP connection using the provided configuration.
func NewConnection(config ConnectionConfig) (*Connection, error) {
	// Establish a connection to the AMQP server
	conn, err := amqp.Dial(config.URI)
	if err != nil {
		return nil, err
	}

	// Open a new channel over the connection
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	// Declare a new queue on the channel
	queueConfig := config.QueueConfig
	if _, err = ch.QueueDeclare(
		queueConfig.Name,
		queueConfig.Durable,
		queueConfig.AutoDelete,
		queueConfig.Exclusive,
		queueConfig.NoWait,
		queueConfig.Args,
	); err != nil {
		_ = conn.Close()
		return nil, err
	}

	// Return the established connection and channel
	return &Connection{conn, ch}, nil
}

func (c *Connection) Close() error {
	return c.Conn.Close()
}
