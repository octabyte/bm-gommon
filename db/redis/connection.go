package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Config holds the configuration for the Redis client
type Config struct {
	Addr              string
	Password          string
	DB                int
	ConnectTimeout    time.Duration     // Per-attempt timeout for initial connection checks
	MaxRetries        int               // Native go-redis command retry count (-1 disables)
	MinRetryBackoff   time.Duration     // Native go-redis minimum retry backoff
	MaxRetryBackoff   time.Duration     // Native go-redis maximum retry backoff
	RetryInitial      time.Duration     // Initial backoff for startup ping retries
	RetryMaxInterval  time.Duration     // Maximum backoff interval for startup ping retries
	RetryMaxElapsed   time.Duration     // Maximum total time to retry startup ping (negative disables retries)
	EnableTracing     bool              // Enable OpenTelemetry tracing
	EnableMetrics     bool              // Enable OpenTelemetry metrics
	ServiceName       string            // Service name for tracing
	ServiceVersion    string            // Service version for tracing
	TracingAttributes map[string]string // Additional tracing attributes
}

func NewRedisClient(ctx context.Context, cfg Config) *redis.Client {
	options := &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	if cfg.ConnectTimeout > 0 {
		options.DialTimeout = cfg.ConnectTimeout
	}
	if cfg.MaxRetries != 0 {
		options.MaxRetries = cfg.MaxRetries
	}
	if cfg.MinRetryBackoff > 0 {
		options.MinRetryBackoff = cfg.MinRetryBackoff
	}
	if cfg.MaxRetryBackoff > 0 {
		options.MaxRetryBackoff = cfg.MaxRetryBackoff
	}

	client := redis.NewClient(options)

	// Configure OpenTelemetry instrumentation if enabled
	if cfg.EnableTracing || cfg.EnableMetrics {
		if err := configureOTelInstrumentation(client, cfg); err != nil {
			log.Printf("Warning: Failed to configure OpenTelemetry instrumentation: %v", err)
		}
	}

	// Ping the Redis server to ensure the connection is established
	if err := pingWithRetry(ctx, client, cfg); err != nil {
		log.Fatalf("Failed to connect to Redis after retries: %v", err)
	}

	return client
}

// pingWithRetry retries the startup ping with exponential backoff.
func pingWithRetry(ctx context.Context, client *redis.Client, cfg Config) error {
	return pingWithRetryFunc(ctx, cfg, func(pingCtx context.Context) error {
		return client.Ping(pingCtx).Err()
	})
}

func pingWithRetryFunc(ctx context.Context, cfg Config, pingFn func(context.Context) error) error {
	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 5 * time.Second
	}

	maxElapsed := cfg.RetryMaxElapsed
	if maxElapsed == 0 {
		maxElapsed = 30 * time.Second
	}
	if maxElapsed < 0 {
		pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
		defer cancel()
		return pingFn(pingCtx)
	}

	initialInterval := cfg.RetryInitial
	if initialInterval <= 0 {
		initialInterval = 500 * time.Millisecond
	}

	maxInterval := cfg.RetryMaxInterval
	if maxInterval <= 0 {
		maxInterval = 5 * time.Second
	}

	start := time.Now()
	backoff := initialInterval
	var lastErr error
	attempt := 0

	for {
		attempt++
		pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
		err := pingFn(pingCtx)
		cancel()
		if err == nil {
			if attempt > 1 {
				log.Printf("Redis connection established after %d attempts", attempt)
			}
			return nil
		}
		lastErr = err

		elapsed := time.Since(start)
		if elapsed >= maxElapsed {
			return fmt.Errorf("startup ping failed after %d attempts in %s: %w", attempt, elapsed.Truncate(time.Millisecond), lastErr)
		}

		wait := backoff
		remaining := maxElapsed - elapsed
		if wait > remaining {
			wait = remaining
		}

		log.Printf("Redis startup ping attempt %d failed: %v (retrying in %s)", attempt, err, wait.Truncate(time.Millisecond))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > maxInterval {
			backoff = maxInterval
		}
	}
}

// configureOTelInstrumentation sets up OpenTelemetry instrumentation for the Redis client
func configureOTelInstrumentation(client *redis.Client, cfg Config) error {
	// Set up tracing instrumentation
	if cfg.EnableTracing {
		opts := []redisotel.TracingOption{}

		// Add service name if provided
		if cfg.ServiceName != "" {
			opts = append(opts, redisotel.WithAttributes(
				attribute.String("service.name", cfg.ServiceName),
			))
		}

		// Add service version if provided
		if cfg.ServiceVersion != "" {
			opts = append(opts, redisotel.WithAttributes(
				attribute.String("service.version", cfg.ServiceVersion),
			))
		}

		// Add custom attributes if provided
		if len(cfg.TracingAttributes) > 0 {
			attrs := make([]attribute.KeyValue, 0, len(cfg.TracingAttributes))
			for key, value := range cfg.TracingAttributes {
				attrs = append(attrs, attribute.String(key, value))
			}
			opts = append(opts, redisotel.WithAttributes(attrs...))
		}

		if err := redisotel.InstrumentTracing(client, opts...); err != nil {
			return fmt.Errorf("failed to instrument Redis tracing: %w", err)
		}
	}

	// Set up metrics instrumentation
	if cfg.EnableMetrics {
		opts := []redisotel.MetricsOption{}

		// Add service name if provided
		if cfg.ServiceName != "" {
			opts = append(opts, redisotel.WithAttributes(
				attribute.String("service.name", cfg.ServiceName),
			))
		}

		if err := redisotel.InstrumentMetrics(client, opts...); err != nil {
			return fmt.Errorf("failed to instrument Redis metrics: %w", err)
		}
	}

	return nil
}

// Ping tests the Redis connection with optional tracing
func Ping(ctx context.Context, client *redis.Client) error {
	tracer := otel.Tracer("redis-client")
	ctx, span := tracer.Start(ctx, "redis.ping")
	defer span.End()

	err := client.Ping(ctx).Err()
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.String("error", err.Error()))
	} else {
		span.SetAttributes(attribute.Bool("success", true))
	}

	return err
}

// NewRedisClientWithTracing creates a new Redis client with tracing enabled by default
func NewRedisClientWithTracing(ctx context.Context, cfg Config, serviceName, serviceVersion string) *redis.Client {
	cfg.EnableTracing = true
	cfg.EnableMetrics = true
	cfg.ServiceName = serviceName
	cfg.ServiceVersion = serviceVersion

	return NewRedisClient(ctx, cfg)
}

// GetTracedClient returns a Redis client with custom tracing configuration
func GetTracedClient(ctx context.Context, cfg Config) (*redis.Client, error) {
	tracer := otel.Tracer("redis-connection")
	ctx, span := tracer.Start(ctx, "redis.connect")
	defer span.End()

	span.SetAttributes(
		attribute.String("redis.addr", cfg.Addr),
		attribute.Int("redis.db", cfg.DB),
		attribute.Bool("tracing.enabled", cfg.EnableTracing),
		attribute.Bool("metrics.enabled", cfg.EnableMetrics),
	)

	client := NewRedisClient(ctx, cfg)

	span.SetAttributes(attribute.Bool("connection.success", true))
	return client, nil
}
