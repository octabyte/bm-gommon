package mongo

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Config holds the configuration for the MongoDB client
type Config struct {
	URI               string            // MongoDB connection URI
	Database          string            // Default database name
	ConnectTimeout    time.Duration     // Connection timeout (default: 10s)
	MaxPoolSize       uint64            // Max connection pool size (default: 100)
	MinPoolSize       uint64            // Min connection pool size (default: 0)
	EnableTracing     bool              // Enable OpenTelemetry tracing
	ServiceName       string            // Service name for tracing
	ServiceVersion    string            // Service version for tracing
	TracingAttributes map[string]string // Additional tracing attributes
}

// NewMongoClient creates a new MongoDB client with optional OpenTelemetry instrumentation
func NewMongoClient(ctx context.Context, cfg Config) (*mongo.Client, error) {
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set defaults
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.MaxPoolSize == 0 {
		cfg.MaxPoolSize = 100
	}

	// Create client options
	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetMaxPoolSize(cfg.MaxPoolSize)

	if cfg.MinPoolSize > 0 {
		clientOpts.SetMinPoolSize(cfg.MinPoolSize)
	}

	// Add OpenTelemetry instrumentation if enabled
	if cfg.EnableTracing {
		monitor := otelmongo.NewMonitor()
		clientOpts.SetMonitor(monitor)
	}

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	log.Printf("Successfully connected to MongoDB: %s", maskURI(cfg.URI))

	return client, nil
}

// NewMongoClientWithTracing creates a new MongoDB client with tracing enabled
func NewMongoClientWithTracing(ctx context.Context, cfg Config, serviceName, serviceVersion string) (*mongo.Client, error) {
	cfg.EnableTracing = true
	cfg.ServiceName = serviceName
	cfg.ServiceVersion = serviceVersion

	return NewMongoClient(ctx, cfg)
}

// GetDatabase returns a database handle with optional tracing
func GetDatabase(ctx context.Context, client *mongo.Client, dbName string, serviceName string) *mongo.Database {
	if serviceName != "" {
		tracer := otel.Tracer(serviceName)
		_, span := tracer.Start(ctx, "mongodb.get_database")
		defer span.End()

		span.SetAttributes(
			attribute.String("db.name", dbName),
			attribute.String("db.system", "mongodb"),
		)
	}

	return client.Database(dbName)
}

// Ping tests the MongoDB connection with optional tracing
func Ping(ctx context.Context, client *mongo.Client, serviceName string) error {
	if serviceName != "" {
		tracer := otel.Tracer(serviceName)
		ctx, span := tracer.Start(ctx, "mongodb.ping")
		defer span.End()

		err := client.Ping(ctx, nil)
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.String("error", err.Error()))
			return err
		}

		span.SetAttributes(attribute.Bool("success", true))
		return nil
	}

	return client.Ping(ctx, nil)
}

// Disconnect gracefully disconnects from MongoDB
func Disconnect(ctx context.Context, client *mongo.Client, serviceName string) error {
	if serviceName != "" {
		tracer := otel.Tracer(serviceName)
		ctx, span := tracer.Start(ctx, "mongodb.disconnect")
		defer span.End()

		err := client.Disconnect(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.String("error", err.Error()))
			return err
		}

		span.SetAttributes(attribute.Bool("success", true))
		return nil
	}

	return client.Disconnect(ctx)
}

// GetTracedClient returns a MongoDB client with custom tracing configuration
func GetTracedClient(ctx context.Context, cfg Config) (*mongo.Client, error) {
	tracer := otel.Tracer("mongodb-connection")
	ctx, span := tracer.Start(ctx, "mongodb.connect")
	defer span.End()

	span.SetAttributes(
		attribute.String("mongodb.uri", maskURI(cfg.URI)),
		attribute.String("mongodb.database", cfg.Database),
		attribute.Bool("tracing.enabled", cfg.EnableTracing),
		attribute.String("service.name", cfg.ServiceName),
	)

	client, err := NewMongoClient(ctx, cfg)
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("connection.success", false))
		return nil, err
	}

	span.SetAttributes(attribute.Bool("connection.success", true))
	return client, nil
}

// Validate validates the MongoDB configuration
func (cfg Config) Validate() error {
	if cfg.URI == "" {
		return fmt.Errorf("MongoDB URI is required")
	}
	if cfg.EnableTracing && cfg.ServiceName == "" {
		return fmt.Errorf("ServiceName is required when tracing is enabled")
	}
	if cfg.MaxPoolSize > 0 && cfg.MinPoolSize > cfg.MaxPoolSize {
		return fmt.Errorf("MinPoolSize (%d) cannot be greater than MaxPoolSize (%d)", cfg.MinPoolSize, cfg.MaxPoolSize)
	}
	return nil
}

// maskURI masks sensitive information in the MongoDB URI for logging
func maskURI(uri string) string {
	// Simple masking - in production you might want more sophisticated masking
	if len(uri) < 20 {
		return "***"
	}
	return uri[:10] + "***" + uri[len(uri)-10:]
}
