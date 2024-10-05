package queue

type ConnectionConfig struct {
	// URI: The RabbitMQ connection URI, which includes the address, port, and authentication credentials if necessary
	URI string
	// QueueConfig: The configuration of the queue associated with the connection
	QueueConfig *Config
}

type Config struct {
	// Name: The name of the queue to be declared and used for message exchange.
	Name string
	// Durable: Indicates whether the queue should be durable (persistent) or not.
	Durable bool
	// AutoDelete: Indicates whether the queue should be automatically deleted when it is no longer in use.
	AutoDelete bool
	// Exclusive: Indicates whether the queue should be exclusive to the connection that declares it.
	Exclusive bool
	// NoWait: Indicates whether the queue declaration should not wait for a response from the server.
	NoWait bool
	// Args: A map of additional arguments to be used when declaring the queue.
	// The supported arguments and their types are listed below:
	// - `x-message-ttl`: The time-to-live for messages in the queue in milliseconds.
	// - `x-max-length`: The maximum length of the queue.
	// - `x-max-length-bytes`: The maximum length of the queue in bytes.
	// - `x-overflow`: The overflow action to be taken when the queue is full.
	// - `x-queue-type`: The type of the queue.
	// - `x-dead-letter-exchange`: The exchange to which messages that are dead-lettered should be routed.
	// - `x-dead-letter-routing-key`: The routing key to use when routing messages that are dead-lettered.
	// - `x-max-priority`: The maximum priority level of messages allowed in the queue.
	// - `x-queue-mode`: The mode of the queue.
	// - `x-queue-master-locator`: The queue master locator to use when creating the queue.
	Args map[string]interface{}
}

type PublishConfig struct {
	// Exchange: The name of the exchange to be used for message publishing.
	Exchange string
	// RoutingKey: The routing key to be used for message publishing.
	RoutingKey string
	// ContentType: The content type of the message to be published.
	// The default value is "application/octet-stream".
	// The supported content types are listed below:
	// - "application/json"
	// - "application/octet-stream"
	// - "application/x-www-form-urlencoded"
	// - "text/plain"
	// - "text/xml"
	// - "text/html"
	// - "application/xml"
	// - "application/x-amqp-sequence"
	// - "application/x-amqp-map"
	// - "application/x-amqp-table"
	// - "application/protobuf"
	// See https://www.rabbitmq.com/amqp-0-9-1-reference.html#content-subtype
	ContentType string
	// DeliveryMode: The delivery mode of the message to be published.
	// 1 = persistent
	// 2 = non-persistent
	DeliveryMode uint8
}

type ConsumeConfig struct {
	// Queue: The name of the queue from which to consume messages.
	Queue string
	// Consumer: The name of the consumer.
	Consumer string
	// AutoAck: Whether the consumer should automatically acknowledge messages.
	AutoAck bool
	// Exclusive: Whether the consumer should be exclusive to the connection that declares it.
	Exclusive bool
	// NoLocal: Whether messages published on the same channel should be ignored.
	NoLocal bool
	// NoWait: Whether the consumer should not wait for a response from the server.
	NoWait bool
	// Args: Additional arguments to be used when consuming from the queue.
	Args map[string]interface{}
}

type ConsumeWithRetryConfig struct {
	// Queue: The name of the queue from which to consume messages.
	Queue string
	// Consumer: The name of the consumer.
	Consumer string
	// Exclusive: Whether the consumer should be exclusive to the connection that declares it.
	Exclusive bool
	// NoLocal: Whether messages published on the same channel should be ignored.
	NoLocal bool
	// NoWait: Whether the consumer should not wait for a response from the server.
	NoWait bool
	// MaxRetries: // maxRetries specifies the maximum number of retry attempts for a message before it is
	// moved to the Dead Letter Queue (DLQ).
	MaxRetries int
	// MessageTTL: // retryIntervalMillis specifies the number of milliseconds to wait before attempting to
	// retry a message.
	MessageTTL int64
	// Args: Additional arguments to be used when consuming from the queue.
	Args map[string]interface{}
}

// See https://www.rabbitmq.com/tutorials/amqp-concepts-tutorial.html
