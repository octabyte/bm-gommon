package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	otelgorm "gorm.io/plugin/opentelemetry/tracing"
)

// Config holds the configuration for the PostgreSQL/GORM client
type Config struct {
	ConnectionString  string            // PostgreSQL connection string (DSN) (required)
	MaxOpenConns      int               // Maximum number of open connections (default: 100)
	MaxIdleConns      int               // Maximum number of idle connections (default: 10)
	ConnMaxLifetime   time.Duration     // Maximum lifetime of a connection (default: 1 hour)
	ConnMaxIdleTime   time.Duration     // Maximum idle time of a connection (default: 10 minutes)
	PrepareStmt       bool              // Enable prepared statement cache (default: true)
	EnableTracing     bool              // Enable OpenTelemetry tracing
	EnableMetrics     bool              // Enable OpenTelemetry metrics
	ServiceName       string            // Service name for tracing
	ServiceVersion    string            // Service version for tracing
	TracingAttributes map[string]string // Additional tracing attributes
	Logger            logger.Interface  // Custom GORM logger (optional)
}

// NewPostgresClient creates a new GORM PostgreSQL client with optional OpenTelemetry instrumentation
func NewPostgresClient(ctx context.Context, cfg Config) (*gorm.DB, error) {
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set defaults
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = 100
	}
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = 10
	}
	if cfg.ConnMaxLifetime == 0 {
		cfg.ConnMaxLifetime = 1 * time.Hour
	}
	if cfg.ConnMaxIdleTime == 0 {
		cfg.ConnMaxIdleTime = 10 * time.Minute
	}
	// PrepareStmt defaults to true (will be set in gorm.Config)

	// Create GORM config
	gormConfig := &gorm.Config{
		PrepareStmt: cfg.PrepareStmt || true, // Default to true if not explicitly set to false
	}

	// Set custom logger if provided
	if cfg.Logger != nil {
		gormConfig.Logger = cfg.Logger
	}

	// Open database connection
	db, err := gorm.Open(postgres.Open(cfg.ConnectionString), gormConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Get underlying sql.DB for connection pool configuration
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying database connection: %w", err)
	}

	// Configure connection pool
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// Test connection
	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	// Configure OpenTelemetry instrumentation if enabled
	if cfg.EnableTracing {
		if err := configureOTelInstrumentation(db, cfg); err != nil {
			log.Printf("Warning: Failed to configure OpenTelemetry instrumentation: %v", err)
		}
	}

	log.Printf("Successfully connected to PostgreSQL")

	return db, nil
}

// NewPostgresClientWithTracing creates a new PostgreSQL client with tracing enabled
func NewPostgresClientWithTracing(ctx context.Context, cfg Config, serviceName, serviceVersion string) (*gorm.DB, error) {
	cfg.EnableTracing = true
	cfg.EnableMetrics = true
	cfg.ServiceName = serviceName
	cfg.ServiceVersion = serviceVersion

	return NewPostgresClient(ctx, cfg)
}

// configureOTelInstrumentation sets up OpenTelemetry instrumentation for GORM
func configureOTelInstrumentation(db *gorm.DB, cfg Config) error {
	// Create tracing plugin with tracer provider
	tracingPlugin := otelgorm.NewPlugin(
		otelgorm.WithTracerProvider(otel.GetTracerProvider()),
	)

	// Use the tracing plugin
	if err := db.Use(tracingPlugin); err != nil {
		return fmt.Errorf("failed to use tracing plugin: %w", err)
	}

	return nil
}

// GetDB returns the underlying sql.DB from GORM
func GetDB(gormDB *gorm.DB) (*sql.DB, error) {
	return gormDB.DB()
}

// Ping tests the PostgreSQL connection with optional tracing
func Ping(ctx context.Context, db *gorm.DB, serviceName string) error {
	if serviceName != "" {
		tracer := otel.Tracer(serviceName)
		_, span := tracer.Start(ctx, "postgres.ping")
		defer span.End()

		sqlDB, err := db.DB()
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.String("error", err.Error()))
			return err
		}

		err = sqlDB.PingContext(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.String("error", err.Error()))
			return err
		}

		span.SetAttributes(attribute.Bool("success", true))
		return nil
	}

	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	return sqlDB.PingContext(ctx)
}

// Close gracefully closes the database connection
func Close(ctx context.Context, db *gorm.DB, serviceName string) error {
	if serviceName != "" {
		tracer := otel.Tracer(serviceName)
		_, span := tracer.Start(ctx, "postgres.close")
		defer span.End()

		sqlDB, err := db.DB()
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.String("error", err.Error()))
			return err
		}

		err = sqlDB.Close()
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.String("error", err.Error()))
			return err
		}

		span.SetAttributes(attribute.Bool("success", true))
		return nil
	}

	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	return sqlDB.Close()
}

// GetTracedClient returns a PostgreSQL client with custom tracing configuration
func GetTracedClient(ctx context.Context, cfg Config) (*gorm.DB, error) {
	tracer := otel.Tracer("postgres-connection")
	ctx, span := tracer.Start(ctx, "postgres.connect")
	defer span.End()

	span.SetAttributes(
		attribute.String("postgres.dsn", maskConnectionString(cfg.ConnectionString)),
		attribute.Bool("tracing.enabled", cfg.EnableTracing),
		attribute.Bool("metrics.enabled", cfg.EnableMetrics),
		attribute.String("service.name", cfg.ServiceName),
		attribute.Int("connection_pool.max_open", cfg.MaxOpenConns),
		attribute.Int("connection_pool.max_idle", cfg.MaxIdleConns),
	)

	db, err := NewPostgresClient(ctx, cfg)
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("connection.success", false))
		return nil, err
	}

	span.SetAttributes(attribute.Bool("connection.success", true))
	return db, nil
}

// Validate validates the PostgreSQL configuration
func (cfg Config) Validate() error {
	if cfg.ConnectionString == "" {
		return fmt.Errorf("ConnectionString is required")
	}
	if cfg.EnableTracing && cfg.ServiceName == "" {
		return fmt.Errorf("ServiceName is required when tracing is enabled")
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns && cfg.MaxOpenConns > 0 {
		return fmt.Errorf("MaxIdleConns (%d) cannot be greater than MaxOpenConns (%d)", cfg.MaxIdleConns, cfg.MaxOpenConns)
	}
	return nil
}

// maskConnectionString masks sensitive information in the connection string for logging
func maskConnectionString(connStr string) string {
	if len(connStr) < 20 {
		return "***"
	}
	return connStr[:10] + "***" + connStr[len(connStr)-10:]
}

// extractDBName attempts to extract the database name from the connection string
func extractDBName(connStr string) string {
	// Simple extraction - in production you might want more sophisticated parsing
	// This is a basic implementation that works for common formats
	// Format: "host=... port=... user=... password=... dbname=mydb sslmode=..."

	// Look for dbname= pattern
	start := 0
	for i := 0; i < len(connStr)-7; i++ {
		if connStr[i:i+7] == "dbname=" {
			start = i + 7
			break
		}
	}

	if start == 0 {
		return "postgres" // default
	}

	// Find the end (space or end of string)
	end := start
	for end < len(connStr) && connStr[end] != ' ' {
		end++
	}

	return connStr[start:end]
}

// Stats returns connection pool statistics
func Stats(db *gorm.DB) (sql.DBStats, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return sql.DBStats{}, err
	}
	return sqlDB.Stats(), nil
}
