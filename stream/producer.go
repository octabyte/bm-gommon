package stream

import (
	"errors"

	"github.com/goccy/go-json"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type Event struct {
	Name        string      `json:"name"`
	Data        interface{} `json:"data"`
	Idempotency string      `json:"idempotency"`
	ContentType string      `json:"contentType"`
}

func ProduceEvent(producer *stream.Producer, event *Event) error {
	if producer == nil {
		return errors.New("producer is nil")
	}

	if event == nil {
		return errors.New("event is nil")
	}

	if event.ContentType == "" {
		event.ContentType = "application/json"
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	message := amqp.NewMessage(data)
	message.Properties = &amqp.MessageProperties{
		CorrelationID: event.Idempotency,
		ContentType:   event.ContentType,
	}

	err = producer.Send(message)
	if err != nil {
		return err
	}

	return nil
}

// ProduceEventAsync executes ProduceEvent in a goroutine and returns a channel that will receive any error that occurs
func ProduceEventAsync(producer *stream.Producer, event *Event) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		err := ProduceEvent(producer, event)
		errChan <- err
		close(errChan)
	}()

	return errChan
}

type ProducerConfig struct {
	ProducerName string
	StreamName   string
}

func CreateProducer(environment *stream.Environment, producerConfig ProducerConfig) (*stream.Producer, error) {
	producer, err := environment.NewProducer(producerConfig.StreamName,
		stream.NewProducerOptions().
			SetProducerName(producerConfig.ProducerName).
			SetCompression(stream.Compression{}.Gzip()),
	)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
