package otel

import (
	"context"
	"testing"
)

func TestInitOpenTelemetry_Disabled(t *testing.T) {
	cfg := OtelConfig{
		Enabled: false,
	}

	shutdown, err := InitOpenTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("InitOpenTelemetry with disabled config should not error: %v", err)
	}

	// Should return a no-op shutdown function
	shutdown()
}

func TestInitOpenTelemetry_Enabled(t *testing.T) {
	cfg := OtelConfig{
		Enabled:     true,
		ServiceName: "test-service",
		Environment: "test",
		Endpoint:    "localhost:4318",
		Headers:     map[string]string{"authorization": "test-key"},
		SampleRate:  1.0,
	}

	shutdown, err := InitOpenTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("InitOpenTelemetry failed: %v", err)
	}

	// Verify shutdown doesn't panic
	shutdown()
}

func TestNewResource(t *testing.T) {
	cfg := OtelConfig{
		ServiceName: "test-service",
		Environment: "test",
	}

	res, err := newResource(cfg)
	if err != nil {
		t.Fatalf("newResource failed: %v", err)
	}

	if res == nil {
		t.Fatal("newResource returned nil resource")
	}
}
