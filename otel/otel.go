package otel

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/hyperdxio/opentelemetry-logs-go/exporters/otlp/otlplogs"
	"github.com/hyperdxio/opentelemetry-logs-go/exporters/otlp/otlplogs/otlplogshttp"
	sdk "github.com/hyperdxio/opentelemetry-logs-go/sdk/logs"
)

// OtelConfig holds the configuration for OpenTelemetry
type OtelConfig struct {
	Enabled     bool              // Enable/disable OpenTelemetry
	Endpoint    string            // OTLP endpoint URL (e.g., "https://localhost:4318")
	ServiceName string            // Name of your service
	Headers     map[string]string // Authentication headers (e.g., {"authorization": "your-api-key"})
	Protocol    string            // Protocol (http/protobuf or grpc)
	Environment string            // Environment (development, production, etc.)
	SampleRate  float64           // Trace sampling rate (0.0 to 1.0, where 1.0 = 100%)
}

var (
	loggerProvider *sdk.LoggerProvider
)

// InitOpenTelemetry initializes OpenTelemetry with tracing, logging, and metrics
func InitOpenTelemetry(ctx context.Context, cfg OtelConfig) (func(), error) {
	if !cfg.Enabled {
		return func() {}, nil
	}

	// Validate configuration
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Create resource with service information
	res, err := newResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Setup tracing
	tracerShutdown, err := setupTracing(ctx, res, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to setup tracing: %w", err)
	}

	// Setup logging
	loggerShutdown, err := setupLogging(ctx, res, cfg)
	if err != nil {
		tracerShutdown(ctx)
		return nil, fmt.Errorf("failed to setup logging: %w", err)
	}

	// Setup metrics
	metricsShutdown, err := setupMetrics(ctx, res, cfg)
	if err != nil {
		tracerShutdown(ctx)
		loggerShutdown(ctx)
		return nil, fmt.Errorf("failed to setup metrics: %w", err)
	}

	// Return a combined shutdown function
	shutdown := func() {
		if err := tracerShutdown(ctx); err != nil {
			fmt.Printf("Error shutting down tracer: %v\n", err)
		}
		if err := loggerShutdown(ctx); err != nil {
			fmt.Printf("Error shutting down logger: %v\n", err)
		}
		if err := metricsShutdown(ctx); err != nil {
			fmt.Printf("Error shutting down metrics: %v\n", err)
		}
	}

	return shutdown, nil
}

// validateConfig validates the OtelConfig struct
func validateConfig(cfg OtelConfig) error {
	if cfg.ServiceName == "" {
		return fmt.Errorf("ServiceName is required")
	}
	if cfg.Endpoint == "" {
		return fmt.Errorf("Endpoint is required")
	}
	if cfg.SampleRate < 0.0 || cfg.SampleRate > 1.0 {
		return fmt.Errorf("SampleRate must be between 0.0 and 1.0, got %f", cfg.SampleRate)
	}
	return nil
}

// newResource creates a resource with service metadata
func newResource(cfg OtelConfig) (*resource.Resource, error) {
	hostName, _ := os.Hostname()

	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion("1.0.0"),
		semconv.DeploymentEnvironment(cfg.Environment),
		semconv.HostName(hostName),
	), nil
}

// setupTracing configures the trace provider
func setupTracing(ctx context.Context, res *resource.Resource, cfg OtelConfig) (func(context.Context) error, error) {
	// Configure exporter options
	exporterOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.Endpoint),
	}

	// Add headers if provided
	if len(cfg.Headers) > 0 {
		exporterOpts = append(exporterOpts, otlptracehttp.WithHeaders(cfg.Headers))
	}

	// Configure TLS based on endpoint scheme
	if len(cfg.Endpoint) > 0 && cfg.Endpoint[:5] != "https" {
		exporterOpts = append(exporterOpts, otlptracehttp.WithInsecure())
	}

	traceExporter, err := otlptracehttp.New(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(res),
		trace.WithSampler(trace.TraceIDRatioBased(cfg.SampleRate)),
	)

	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return traceProvider.Shutdown, nil
}

// setupLogging configures the logger provider
func setupLogging(ctx context.Context, res *resource.Resource, cfg OtelConfig) (func(context.Context) error, error) {
	// Configure HTTP client options
	clientOpts := []otlplogshttp.Option{
		otlplogshttp.WithEndpoint(cfg.Endpoint),
	}

	// Add headers if provided
	if len(cfg.Headers) > 0 {
		clientOpts = append(clientOpts, otlplogshttp.WithHeaders(cfg.Headers))
	}

	// Configure TLS based on endpoint scheme
	if len(cfg.Endpoint) > 0 && cfg.Endpoint[:5] != "https" {
		clientOpts = append(clientOpts, otlplogshttp.WithInsecure())
	}

	// Create HTTP client
	client := otlplogshttp.NewClient(clientOpts...)

	// Create exporter with the client
	logExporter, err := otlplogs.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to create log exporter: %w", err)
	}

	loggerProvider = sdk.NewLoggerProvider(
		sdk.WithBatcher(logExporter),
		sdk.WithResource(res),
	)

	return loggerProvider.Shutdown, nil
}

// setupMetrics configures the metrics provider
func setupMetrics(ctx context.Context, res *resource.Resource, cfg OtelConfig) (func(context.Context) error, error) {
	// Configure exporter options
	exporterOpts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(cfg.Endpoint),
	}

	// Add headers if provided
	if len(cfg.Headers) > 0 {
		exporterOpts = append(exporterOpts, otlpmetrichttp.WithHeaders(cfg.Headers))
	}

	// Configure TLS based on endpoint scheme
	if len(cfg.Endpoint) > 0 && cfg.Endpoint[:5] != "https" {
		exporterOpts = append(exporterOpts, otlpmetrichttp.WithInsecure())
	}

	metricExporter, err := otlpmetrichttp.New(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
		metric.WithResource(res),
	)

	otel.SetMeterProvider(meterProvider)

	return meterProvider.Shutdown, nil
}

// GetLoggerProvider returns the global logger provider
func GetLoggerProvider() *sdk.LoggerProvider {
	return loggerProvider
}
