package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
