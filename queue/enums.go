package queue

type QueueType string

const (
	QueueTypeClassic QueueType = "classic" // Default RabbitMQ queue
	QueueTypeQuorum  QueueType = "quorum"  // High availability, data safety
	QueueTypeStream  QueueType = "stream"  // High-throughput streaming
)
