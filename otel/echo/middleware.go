package echo

import (
	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Middleware returns an Echo middleware that instruments HTTP requests with OpenTelemetry
func Middleware(serviceName string) echo.MiddlewareFunc {
	// Use the official otelecho middleware as the base
	baseMiddleware := otelecho.Middleware(serviceName)

	// Wrap it with custom logic to add additional attributes
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Call the base middleware first
			handler := baseMiddleware(next)
			err := handler(c)

			// Add custom span attributes
			span := trace.SpanFromContext(c.Request().Context())
			if span.IsRecording() {
				// Add request information
				span.SetAttributes(
					attribute.String("http.route", c.Path()),
					attribute.String("http.method", c.Request().Method),
					attribute.Int("http.status_code", c.Response().Status),
				)

				// Add user ID if available from context (JWT token)
				if token := c.Get("requestToken"); token != nil {
					if tokenStr, ok := token.(string); ok && tokenStr != "" {
						span.SetAttributes(attribute.String("user.token_present", "true"))
					}
				}

				// Add error information if any
				if err != nil {
					span.SetAttributes(attribute.String("error.message", err.Error()))
				}
			}

			return err
		}
	}
}

// MiddlewareWithConfig returns an Echo middleware with custom configuration
func MiddlewareWithConfig(serviceName string, skipper func(c echo.Context) bool) echo.MiddlewareFunc {
	baseMiddleware := otelecho.Middleware(serviceName)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Skip middleware if configured
			if skipper != nil && skipper(c) {
				return next(c)
			}

			handler := baseMiddleware(next)
			err := handler(c)

			span := trace.SpanFromContext(c.Request().Context())
			if span.IsRecording() {
				span.SetAttributes(
					attribute.String("http.route", c.Path()),
					attribute.String("http.method", c.Request().Method),
					attribute.Int("http.status_code", c.Response().Status),
				)

				if token := c.Get("requestToken"); token != nil {
					if tokenStr, ok := token.(string); ok && tokenStr != "" {
						span.SetAttributes(attribute.String("user.token_present", "true"))
					}
				}

				if err != nil {
					span.SetAttributes(attribute.String("error.message", err.Error()))
				}
			}

			return err
		}
	}
}
