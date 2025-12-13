package mongo

import (
	"context"
	"testing"
	"time"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				URI:         "mongodb://localhost:27017",
				Database:    "testdb",
				ServiceName: "test-service",
			},
			wantErr: false,
		},
		{
			name: "missing URI",
			cfg: Config{
				Database: "testdb",
			},
			wantErr: true,
		},
		{
			name: "tracing enabled without service name",
			cfg: Config{
				URI:           "mongodb://localhost:27017",
				EnableTracing: true,
			},
			wantErr: true,
		},
		{
			name: "min pool size greater than max pool size",
			cfg: Config{
				URI:         "mongodb://localhost:27017",
				MinPoolSize: 100,
				MaxPoolSize: 50,
			},
			wantErr: true,
		},
		{
			name: "valid with tracing",
			cfg: Config{
				URI:           "mongodb://localhost:27017",
				EnableTracing: true,
				ServiceName:   "test-service",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMaskURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{
			name: "short URI",
			uri:  "mongodb://local",
			want: "***",
		},
		{
			name: "long URI",
			uri:  "mongodb://username:password@localhost:27017/mydb",
			want: "mongodb://***27017/mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskURI(tt.uri)
			if got != tt.want {
				t.Errorf("maskURI() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{
		URI:      "mongodb://localhost:27017",
		Database: "testdb",
	}

	// Validate doesn't fail with missing optional fields
	if err := cfg.Validate(); err != nil {
		t.Errorf("Config.Validate() with defaults should not error: %v", err)
	}
}

func TestNewMongoClient_ValidationError(t *testing.T) {
	ctx := context.Background()

	// Test with invalid config (missing URI)
	cfg := Config{
		Database: "testdb",
	}

	_, err := NewMongoClient(ctx, cfg)
	if err == nil {
		t.Error("NewMongoClient() should return error for invalid config")
	}
}

func TestConfig_ValidatePoolSizes(t *testing.T) {
	tests := []struct {
		name        string
		minPoolSize uint64
		maxPoolSize uint64
		wantErr     bool
	}{
		{
			name:        "min equals max",
			minPoolSize: 50,
			maxPoolSize: 50,
			wantErr:     false,
		},
		{
			name:        "min less than max",
			minPoolSize: 10,
			maxPoolSize: 100,
			wantErr:     false,
		},
		{
			name:        "both zero",
			minPoolSize: 0,
			maxPoolSize: 0,
			wantErr:     false,
		},
		{
			name:        "only max set",
			minPoolSize: 0,
			maxPoolSize: 100,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				URI:         "mongodb://localhost:27017",
				MinPoolSize: tt.minPoolSize,
				MaxPoolSize: tt.maxPoolSize,
			}

			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Note: Integration tests that require actual MongoDB connection
// are intentionally not included here. Those should be in a separate
// integration test file that can be run with build tags.

func TestConfigTracingRequirements(t *testing.T) {
	tests := []struct {
		name          string
		enableTracing bool
		serviceName   string
		wantErr       bool
	}{
		{
			name:          "tracing disabled, no service name",
			enableTracing: false,
			serviceName:   "",
			wantErr:       false,
		},
		{
			name:          "tracing enabled with service name",
			enableTracing: true,
			serviceName:   "my-service",
			wantErr:       false,
		},
		{
			name:          "tracing enabled without service name",
			enableTracing: true,
			serviceName:   "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				URI:           "mongodb://localhost:27017",
				EnableTracing: tt.enableTracing,
				ServiceName:   tt.serviceName,
			}

			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigWithTimeout(t *testing.T) {
	cfg := Config{
		URI:            "mongodb://localhost:27017",
		ConnectTimeout: 5 * time.Second,
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Config with timeout should be valid: %v", err)
	}
}
