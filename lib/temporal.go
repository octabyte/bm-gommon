package lib

import (
	"context"
	"github.com/go-playground/validator/v10"
	"go.temporal.io/sdk/client"
)

type TemporalClientConfig struct {
	HostPort  string `validate:"required"`
	Namespace string
}

type ExecuteWorkflowOptions struct {
	ID        string      `validate:"required"`
	TaskQueue string      `validate:"required"`
	Input     interface{} `validate:"required"`
	Type      string      `validate:"required"`
}

func (cfg *TemporalClientConfig) Validate() error {
	return validator.New(validator.WithRequiredStructEnabled()).Struct(cfg)
}

func (options *ExecuteWorkflowOptions) Validate() error {
	return validator.New(validator.WithRequiredStructEnabled()).Struct(options)
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

func ExecuteWorkflow(ctx context.Context, tClient client.Client, options ExecuteWorkflowOptions) error {
	if err := options.Validate(); err != nil {
		return err
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:        options.ID,
		TaskQueue: options.TaskQueue,
	}

	_, err := tClient.ExecuteWorkflow(ctx, workflowOptions, options.Type, options.Input)
	if err != nil {
		return err
	}

	return nil
}
