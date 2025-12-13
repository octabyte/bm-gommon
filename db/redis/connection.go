package redis

import (
	"context"
	"fmt"
	"log"

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

	client := redis.NewClient(options)

	// Configure OpenTelemetry instrumentation if enabled
	if cfg.EnableTracing || cfg.EnableMetrics {
		if err := configureOTelInstrumentation(client, cfg); err != nil {
			log.Printf("Warning: Failed to configure OpenTelemetry instrumentation: %v", err)
		}
	}

	// Ping the Redis server to ensure the connection is established
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	return client
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
