package connection

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	URI string
}

type Connection struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

// NewConnection creates a new AMQP connection using the provided configuration.
func NewConnection(config Config) (*Connection, error) {
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

	return &Connection{conn, ch}, nil
}

func (c *Connection) Close() error {
	return c.Conn.Close()
}
