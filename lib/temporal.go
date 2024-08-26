package lib

import (
	"github.com/go-playground/validator/v10"
	"go.temporal.io/sdk/client"
)

type TemporalClientConfig struct {
	HostPort  string `validate:"required"`
	Namespace string
}

func (cfg *TemporalClientConfig) Validate() error {
	return validator.New(validator.WithRequiredStructEnabled()).Struct(cfg)
}

func NewTemporalClient(cfg *TemporalClientConfig) (client.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort: cfg.HostPort,
	})
	if err != nil {
		return nil, err
	}

	return temporalClient, nil
}
