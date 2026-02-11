package lib

import (
	"context"
	"time"

	"github.com/go-playground/validator/v10"
	"go.temporal.io/sdk/client"
)

// TemporalClientConfig holds settings for Temporal client connectivity.
type TemporalClientConfig struct {
	HostPort             string        `validate:"required"` // Temporal frontend host:port.
	Namespace            string        // Temporal namespace to use.
	DialTimeout          time.Duration // Per-attempt timeout for dialing Temporal.
	DialRetryInitial     time.Duration // Initial backoff interval for dial retries.
	DialRetryMaxInterval time.Duration // Maximum backoff interval for dial retries.
	DialRetryMaxElapsed  time.Duration // Maximum total time to retry dialing; 0 uses default.
	UseLazyConnect       bool          // Use a lazy client to defer connecting until first use.
}

// ExecuteWorkflowOptions describes workflow execution options.
type ExecuteWorkflowOptions struct {
	ID        string      `validate:"required"`
	TaskQueue string      `validate:"required"`
	Input     interface{} `validate:"required"`
	Type      string      `validate:"required"`
}

// Validate ensures the Temporal client config is valid.
func (cfg *TemporalClientConfig) Validate() error {
	return validator.New(validator.WithRequiredStructEnabled()).Struct(cfg)
}

// Validate ensures workflow execution options are valid.
func (options *ExecuteWorkflowOptions) Validate() error {
	return validator.New(validator.WithRequiredStructEnabled()).Struct(options)
}

// NewTemporalClient builds a Temporal client with retry-aware dialing.
func NewTemporalClient(cfg *TemporalClientConfig) (client.Client, error) {
	return NewTemporalClientWithContext(context.Background(), cfg)
}

// NewTemporalClientWithContext builds a Temporal client with dial retries.
func NewTemporalClientWithContext(ctx context.Context, cfg *TemporalClientConfig) (client.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	options := client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
	}

	if cfg.UseLazyConnect {
		return client.NewLazyClient(options)
	}

	return dialTemporalClientWithRetry(ctx, options, cfg)
}

// dialTemporalClientWithRetry attempts to connect with exponential backoff.
func dialTemporalClientWithRetry(ctx context.Context, options client.Options, cfg *TemporalClientConfig) (client.Client, error) {
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 10 * time.Second
	}

	maxElapsed := cfg.DialRetryMaxElapsed
	if maxElapsed < 0 {
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return client.DialContext(dialCtx, options)
	}
	if maxElapsed == 0 {
		maxElapsed = 2 * time.Minute
	}

	initialInterval := cfg.DialRetryInitial
	if initialInterval <= 0 {
		initialInterval = time.Second
	}

	maxInterval := cfg.DialRetryMaxInterval
	if maxInterval <= 0 {
		maxInterval = 30 * time.Second
	}

	start := time.Now()
	backoff := initialInterval
	var lastErr error

	for {
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		temporalClient, err := client.DialContext(dialCtx, options)
		cancel()
		if err == nil {
			return temporalClient, nil
		}
		lastErr = err

		elapsed := time.Since(start)
		if elapsed >= maxElapsed {
			return nil, lastErr
		}

		wait := backoff
		remaining := maxElapsed - elapsed
		if wait > remaining {
			wait = remaining
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > maxInterval {
			backoff = maxInterval
		}
	}
}

// ExecuteWorkflow runs a workflow with validated options.
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
