package otel

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-resty/resty/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// StartHTTPSpan creates a span for HTTP client calls with standard attributes
// Returns the updated context and a finish function to be called after the HTTP request completes
func StartHTTPSpan(ctx context.Context, serviceName string, clientName string, operation string, method string, baseURL string, url string) (context.Context, func(statusCode int, err error)) {
	tracer := otel.Tracer(serviceName)
	spanName := fmt.Sprintf("HTTP.%s.%s", clientName, operation)
	ctx, span := tracer.Start(ctx, spanName)

	span.SetAttributes(
		semconv.HTTPRequestMethodKey.String(method),
		semconv.URLFull(baseURL+url),
		attribute.String("http.target", url),
	)

	return ctx, func(statusCode int, err error) {
		defer span.End()

		if statusCode > 0 {
			span.SetAttributes(semconv.HTTPResponseStatusCodeKey.Int(statusCode))
		}

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else if statusCode >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
		} else {
			span.SetStatus(codes.Ok, "success")
		}
	}
}

// InjectTraceHeaders injects W3C trace context headers (traceparent, tracestate) into an HTTP request
// This ensures distributed tracing works across service boundaries
func InjectTraceHeaders(ctx context.Context, headers map[string]string) map[string]string {
	if headers == nil {
		headers = make(map[string]string)
	}

	// Use the global text map propagator to inject trace context
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headers)
	propagator.Inject(ctx, carrier)

	return headers
}

// InjectTraceHeadersIntoRequest injects W3C trace context headers into a standard http.Request
func InjectTraceHeadersIntoRequest(ctx context.Context, req *http.Request) {
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))
}

// WithTraceHeaders is a Resty middleware that automatically injects trace headers into all requests
// Usage: client.OnBeforeRequest(otel.WithTraceHeaders)
func WithTraceHeaders(client *resty.Client, req *resty.Request) error {
	ctx := req.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	// Inject trace headers into the request
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))

	return nil
}

// NewTracedRestyClient creates a new Resty client with automatic trace propagation
// This client will automatically inject traceparent and tracestate headers into all outbound requests
func NewTracedRestyClient(baseURL string) *resty.Client {
	client := resty.New().
		SetBaseURL(baseURL).
		OnBeforeRequest(WithTraceHeaders)

	return client
}
