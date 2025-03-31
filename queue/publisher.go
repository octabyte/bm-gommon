package queue

import amqp "github.com/rabbitmq/amqp091-go"

func (r *RabbitMQ) Publish(queueName string, body string) error {
	return r.channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
}
