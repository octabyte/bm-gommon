package stream

import (
	"errors"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type EnvironmentConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

func NewEnvironment(environmentConfig EnvironmentConfig) (*stream.Environment, error) {
	if environmentConfig.Host == "" {
		return nil, errors.New("host is required")
	}

	if environmentConfig.Port == 0 {
		return nil, errors.New("port is required")
	}

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost(environmentConfig.Host).
			SetPort(environmentConfig.Port).
			SetUser(environmentConfig.User).
			SetPassword(environmentConfig.Password))
	if err != nil {
		return nil, err
	}

	return env, nil
}
