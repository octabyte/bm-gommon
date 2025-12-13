package otel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func setupTestTracer() func() {
	// Create a test tracer provider
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)

	// Set up the W3C propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() {
		_ = tp.Shutdown(context.Background())
	}
}

func TestInjectTraceHeaders(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a span
	tracer := otel.Tracer("test-service")
	ctx, span := tracer.Start(context.Background(), "test-operation")
	defer span.End()

	// Inject trace headers
	headers := make(map[string]string)
	headers = InjectTraceHeaders(ctx, headers)

	// Verify traceparent header exists
	assert.Contains(t, headers, "traceparent", "traceparent header should be injected")

	// Verify traceparent format (00-{trace-id}-{span-id}-{flags})
	traceparent := headers["traceparent"]
	assert.NotEmpty(t, traceparent, "traceparent should not be empty")

	// Extract span context to verify trace ID
	spanContext := span.SpanContext()
	assert.True(t, spanContext.IsValid(), "span context should be valid")
}

func TestInjectTraceHeadersIntoRequest(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a span
	tracer := otel.Tracer("test-service")
	ctx, span := tracer.Start(context.Background(), "test-operation")
	defer span.End()

	// Create an HTTP request
	req := httptest.NewRequest(http.MethodGet, "/test", nil)

	// Inject trace headers into the request
	InjectTraceHeadersIntoRequest(ctx, req)

	// Verify traceparent header exists
	traceparent := req.Header.Get("traceparent")
	assert.NotEmpty(t, traceparent, "traceparent header should be injected into request")
}

func TestWithTraceHeaders(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a test server that echoes back headers
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return the traceparent header if it exists
		traceparent := r.Header.Get("traceparent")
		w.Header().Set("X-Received-Traceparent", traceparent)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create a span
	tracer := otel.Tracer("test-service")
	ctx, span := tracer.Start(context.Background(), "test-operation")
	defer span.End()

	// Create a Resty client with trace middleware
	client := resty.New().
		SetBaseURL(server.URL).
		OnBeforeRequest(WithTraceHeaders)

	// Make a request
	resp, err := client.R().SetContext(ctx).Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())

	// Verify the server received the traceparent header
	receivedTraceparent := resp.Header().Get("X-Received-Traceparent")
	assert.NotEmpty(t, receivedTraceparent, "server should have received traceparent header")
}

func TestNewTracedRestyClient(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a test server that echoes back headers
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent := r.Header.Get("traceparent")
		w.Header().Set("X-Received-Traceparent", traceparent)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create a span
	tracer := otel.Tracer("test-service")
	ctx, span := tracer.Start(context.Background(), "test-operation")
	defer span.End()

	// Create a traced Resty client
	client := NewTracedRestyClient(server.URL)

	// Make a request
	resp, err := client.R().SetContext(ctx).Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())

	// Verify the server received the traceparent header
	receivedTraceparent := resp.Header().Get("X-Received-Traceparent")
	assert.NotEmpty(t, receivedTraceparent, "server should have received traceparent header")
}

func TestStartHTTPSpan(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	ctx := context.Background()
	serviceName := "test-service"
	clientName := "test-client"
	operation := "get-user"
	method := "GET"
	baseURL := "https://api.example.com"
	url := "/users/123"

	// Start an HTTP span
	spanCtx, finish := StartHTTPSpan(ctx, serviceName, clientName, operation, method, baseURL, url)

	// Verify span context is valid
	span := trace.SpanFromContext(spanCtx)
	assert.True(t, span.SpanContext().IsValid(), "span context should be valid")

	// Simulate successful request
	statusCode := 200
	finish(statusCode, nil)

	// Note: We can't easily verify span attributes without access to the exporter,
	// but we can verify the function doesn't panic and returns valid context
}

func TestStartHTTPSpanWithError(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	ctx := context.Background()

	// Start an HTTP span
	spanCtx, finish := StartHTTPSpan(ctx, "test-service", "test-client", "get-user", "GET", "https://api.example.com", "/users/123")

	// Verify span context is valid
	span := trace.SpanFromContext(spanCtx)
	assert.True(t, span.SpanContext().IsValid(), "span context should be valid")

	// Simulate failed request
	finish(500, assert.AnError)
}

func TestInjectTraceHeadersWithNilMap(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a span
	tracer := otel.Tracer("test-service")
	ctx, span := tracer.Start(context.Background(), "test-operation")
	defer span.End()

	// Inject trace headers with nil map
	headers := InjectTraceHeaders(ctx, nil)

	// Verify headers map was created and traceparent exists
	assert.NotNil(t, headers, "headers map should be created")
	assert.Contains(t, headers, "traceparent", "traceparent header should be injected")
}

func TestTraceContextPropagation(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a parent span
	tracer := otel.Tracer("test-service")
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent-operation")
	defer parentSpan.End()

	parentSpanContext := parentSpan.SpanContext()

	// Inject headers from parent span
	headers := make(map[string]string)
	headers = InjectTraceHeaders(parentCtx, headers)

	// Simulate extracting headers on receiving side
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headers)
	childCtx := propagator.Extract(context.Background(), carrier)

	// Create a child span from extracted context
	childCtx, childSpan := tracer.Start(childCtx, "child-operation")
	defer childSpan.End()

	childSpanContext := childSpan.SpanContext()

	// Verify trace IDs match (distributed tracing)
	assert.Equal(t, parentSpanContext.TraceID(), childSpanContext.TraceID(),
		"parent and child should have the same trace ID")

	// Verify span IDs are different (different operations)
	assert.NotEqual(t, parentSpanContext.SpanID(), childSpanContext.SpanID(),
		"parent and child should have different span IDs")
}
