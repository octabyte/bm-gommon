package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	tContainer "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type RabbitMQTestSuite struct {
	suite.Suite
	ctx       context.Context
	container tContainer.Container
	rmq       *RabbitMQ
}

func (s *RabbitMQTestSuite) SetupSuite() {
	s.ctx = context.Background()

	req := tContainer.ContainerRequest{
		Image:        "rabbitmq:3-management",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete"),
	}
	container, err := tContainer.GenericContainer(s.ctx, tContainer.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.Require().NoError(err)

	s.container = container

	host, err := container.Host(s.ctx)
	s.Require().NoError(err)

	port, err := container.MappedPort(s.ctx, "5672")
	s.Require().NoError(err)

	url := fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())
	rmq, err := New(url)
	s.Require().NoError(err)

	s.rmq = rmq
}

func (s *RabbitMQTestSuite) TearDownSuite() {
	s.rmq.Close()
	s.Require().NoError(s.container.Terminate(s.ctx))
}

func (s *RabbitMQTestSuite) TestQueueDeclarePublishConsume() {
	queueCfg := QueueConfig{
		Name:              "test.jobs",
		Type:              QueueTypeClassic,
		Durable:           true,
		DeadLetterEnabled: true,
		DeadLetterSuffix:  ".dlq",
		TTL:               5 * time.Second,
	}

	queue, err := s.rmq.DeclareQueue(queueCfg)
	s.Require().NoError(err)

	err = s.rmq.Publish(queue.Name, "hello")
	s.Require().NoError(err)

	msgs, err := s.rmq.Consume(queue.Name, false)
	s.Require().NoError(err)

	select {
	case msg := <-msgs:
		s.Equal("hello", string(msg.Body))
		msg.Ack(false)
	case <-time.After(3 * time.Second):
		s.Fail("message not received")
	}
}

func (s *RabbitMQTestSuite) TestDeadLetterRouting() {
	queueCfg := QueueConfig{
		Name:              "test.jobs2",
		Type:              QueueTypeClassic,
		Durable:           true,
		DeadLetterEnabled: true,
		DeadLetterSuffix:  ".dlq",
		TTL:               1 * time.Second,
	}

	queue, _ := s.rmq.DeclareQueue(queueCfg)
	dlqName := queueCfg.Name + queueCfg.DeadLetterSuffix

	_ = s.rmq.Publish(queue.Name, "fail-message")

	msgs, _ := s.rmq.Consume(queue.Name, false)
	select {
	case msg := <-msgs:
		msg.Nack(false, false) // dead-letter it
	case <-time.After(3 * time.Second):
		s.Fail("original message not received")
	}

	time.Sleep(1 * time.Second)

	dlqMsgs, _ := s.rmq.Consume(dlqName, true)
	select {
	case dlqMsg := <-dlqMsgs:
		s.Equal("fail-message", string(dlqMsg.Body))
	case <-time.After(2 * time.Second):
		s.Fail("message not routed to DLQ")
	}
}

func TestRabbitMQSuite(t *testing.T) {
	suite.Run(t, new(RabbitMQTestSuite))
}
