package logger

import (
	"context"
	"fmt"

	"github.com/octabyte/bm-gommon/utils/logger"
	"go.opentelemetry.io/otel/trace"
)

// InfoCtx logs an info message with trace context
func InfoCtx(ctx context.Context, msg string, args ...interface{}) {
	logWithTrace(ctx, "info", msg, args...)
}

// ErrorCtx logs an error message with trace context
func ErrorCtx(ctx context.Context, msg string, err error) {
	if err != nil {
		logWithTrace(ctx, "error", fmt.Sprintf("%s: %v", msg, err))
	} else {
		logWithTrace(ctx, "error", msg)
	}
}

// WarnCtx logs a warning message with trace context
func WarnCtx(ctx context.Context, msg string, args ...interface{}) {
	logWithTrace(ctx, "warn", msg, args...)
}

// DebugCtx logs a debug message with trace context
func DebugCtx(ctx context.Context, msg string, args ...interface{}) {
	logWithTrace(ctx, "debug", msg, args...)
}

// InfofCtx logs a formatted info message with trace context
func InfofCtx(ctx context.Context, format string, args ...interface{}) {
	logWithTrace(ctx, "info", fmt.Sprintf(format, args...))
}

// ErrorfCtx logs a formatted error message with trace context
func ErrorfCtx(ctx context.Context, format string, args ...interface{}) {
	logWithTrace(ctx, "error", fmt.Sprintf(format, args...))
}

// WarnfCtx logs a formatted warning message with trace context
func WarnfCtx(ctx context.Context, format string, args ...interface{}) {
	logWithTrace(ctx, "warn", fmt.Sprintf(format, args...))
}

// DebugfCtx logs a formatted debug message with trace context
func DebugfCtx(ctx context.Context, format string, args ...interface{}) {
	logWithTrace(ctx, "debug", fmt.Sprintf(format, args...))
}

// logWithTrace extracts trace information from context and logs with it
func logWithTrace(ctx context.Context, level string, msg string, args ...interface{}) {
	spanContext := trace.SpanContextFromContext(ctx)

	var finalMsg string
	if len(args) > 0 {
		finalMsg = fmt.Sprintf(msg, args...)
	} else {
		finalMsg = msg
	}

	if spanContext.IsValid() {
		// Add trace_id and span_id to the log message
		finalMsg = fmt.Sprintf("[trace_id=%s span_id=%s] %s",
			spanContext.TraceID().String(),
			spanContext.SpanID().String(),
			finalMsg,
		)
	}

	// Use the existing bm-gommon logger based on level
	switch level {
	case "info":
		logger.LogInfo(finalMsg)
	case "error":
		logger.LogError(finalMsg)
	case "warn":
		logger.LogWarn(finalMsg)
	case "debug":
		logger.LogDebug(finalMsg)
	default:
		logger.LogInfo(finalMsg)
	}
}

// WithTraceFields returns a formatted string with trace information
func WithTraceFields(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		return fmt.Sprintf("trace_id=%s span_id=%s",
			spanContext.TraceID().String(),
			spanContext.SpanID().String(),
		)
	}
	return ""
}

// GetTraceID extracts the trace ID from context
func GetTraceID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		return spanContext.TraceID().String()
	}
	return ""
}

// GetSpanID extracts the span ID from context
func GetSpanID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		return spanContext.SpanID().String()
	}
	return ""
}
