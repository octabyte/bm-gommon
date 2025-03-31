package queue

import amqp "github.com/rabbitmq/amqp091-go"

func (r *RabbitMQ) Consume(queueName string, autoAck bool) (<-chan amqp.Delivery, error) {
	return r.channel.Consume(
		queueName,
		"",
		autoAck,
		false,
		false,
		false,
		nil,
	)
}
