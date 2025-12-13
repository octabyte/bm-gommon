package metrics

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter metric.Meter

	// HTTP metrics
	httpRequestsTotal    metric.Int64Counter
	httpRequestDuration  metric.Float64Histogram
	httpRequestsInFlight metric.Int64UpDownCounter
	httpRequestSize      metric.Int64Histogram
	httpResponseSize     metric.Int64Histogram

	// Runtime metrics
	goGoroutines      metric.Int64ObservableGauge
	goMemoryUsage     metric.Int64ObservableGauge
	goMemoryAllocated metric.Int64ObservableGauge
	goGCPauseTotal    metric.Float64ObservableCounter
)

// Init initializes the metrics
func Init(serviceName string) error {
	meter = otel.Meter(serviceName)

	var err error

	// Initialize HTTP metrics
	httpRequestsTotal, err = meter.Int64Counter(
		"http_requests_total",
		metric.WithDescription("Total number of HTTP requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create http_requests_total counter: %w", err)
	}

	httpRequestDuration, err = meter.Float64Histogram(
		"http_request_duration_seconds",
		metric.WithDescription("HTTP request duration in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("failed to create http_request_duration_seconds histogram: %w", err)
	}

	httpRequestsInFlight, err = meter.Int64UpDownCounter(
		"http_requests_in_flight",
		metric.WithDescription("Number of HTTP requests currently in flight"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create http_requests_in_flight gauge: %w", err)
	}

	httpRequestSize, err = meter.Int64Histogram(
		"http_request_size_bytes",
		metric.WithDescription("HTTP request size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("failed to create http_request_size_bytes histogram: %w", err)
	}

	httpResponseSize, err = meter.Int64Histogram(
		"http_response_size_bytes",
		metric.WithDescription("HTTP response size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("failed to create http_response_size_bytes histogram: %w", err)
	}

	// Initialize runtime metrics
	goGoroutines, err = meter.Int64ObservableGauge(
		"go_goroutines",
		metric.WithDescription("Number of goroutines currently running"),
		metric.WithUnit("{goroutine}"),
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			observer.Observe(int64(runtime.NumGoroutine()))
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create go_goroutines gauge: %w", err)
	}

	goMemoryUsage, err = meter.Int64ObservableGauge(
		"go_memory_usage_bytes",
		metric.WithDescription("Memory usage in bytes"),
		metric.WithUnit("By"),
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			observer.Observe(int64(m.Alloc))
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create go_memory_usage_bytes gauge: %w", err)
	}

	goMemoryAllocated, err = meter.Int64ObservableGauge(
		"go_memory_allocated_bytes",
		metric.WithDescription("Total memory allocated in bytes"),
		metric.WithUnit("By"),
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			observer.Observe(int64(m.TotalAlloc))
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create go_memory_allocated_bytes gauge: %w", err)
	}

	goGCPauseTotal, err = meter.Float64ObservableCounter(
		"go_gc_pause_total_seconds",
		metric.WithDescription("Total GC pause time in seconds"),
		metric.WithUnit("s"),
		metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			observer.Observe(float64(m.PauseTotalNs) / 1e9)
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create go_gc_pause_total_seconds counter: %w", err)
	}

	return nil
}

// RecordHTTPRequest records an HTTP request with its metrics
func RecordHTTPRequest(ctx context.Context, method, route string, statusCode int, duration time.Duration, requestSize, responseSize int64) {
	attrs := []attribute.KeyValue{
		attribute.String("http.method", method),
		attribute.String("http.route", route),
		attribute.Int("http.status_code", statusCode),
	}

	if httpRequestsTotal != nil {
		httpRequestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	}

	if httpRequestDuration != nil {
		httpRequestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}

	if httpRequestSize != nil && requestSize > 0 {
		httpRequestSize.Record(ctx, requestSize, metric.WithAttributes(attrs...))
	}

	if httpResponseSize != nil && responseSize > 0 {
		httpResponseSize.Record(ctx, responseSize, metric.WithAttributes(attrs...))
	}
}

// IncrementInFlightRequests increments the in-flight requests counter
func IncrementInFlightRequests(ctx context.Context, method, route string) {
	if httpRequestsInFlight != nil {
		attrs := []attribute.KeyValue{
			attribute.String("http.method", method),
			attribute.String("http.route", route),
		}
		httpRequestsInFlight.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// DecrementInFlightRequests decrements the in-flight requests counter
func DecrementInFlightRequests(ctx context.Context, method, route string) {
	if httpRequestsInFlight != nil {
		attrs := []attribute.KeyValue{
			attribute.String("http.method", method),
			attribute.String("http.route", route),
		}
		httpRequestsInFlight.Add(ctx, -1, metric.WithAttributes(attrs...))
	}
}

// RecordDownstreamCall records metrics for calls to downstream services
func RecordDownstreamCall(ctx context.Context, serviceName string, duration time.Duration, success bool) {
	counter, err := meter.Int64Counter(
		"downstream_calls_total",
		metric.WithDescription("Total number of downstream service calls"),
		metric.WithUnit("{call}"),
	)
	if err != nil {
		return
	}

	histogram, err := meter.Float64Histogram(
		"downstream_call_duration_seconds",
		metric.WithDescription("Downstream service call duration in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
		attribute.Bool("success", success),
	}

	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
	histogram.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}
