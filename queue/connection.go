package queue

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	Conn *amqp.Connection
}

// NewConnection creates a new AMQP connection using the provided configuration.
func NewConnection(config ConnectionConfig) (*Connection, error) {
	// Establish a connection to the AMQP server
	conn, err := amqp.Dial(config.URI)
	if err != nil {
		return nil, err
	}

	return &Connection{conn}, nil
}

func (c *Connection) Close() error {
	return c.Conn.Close()
}
