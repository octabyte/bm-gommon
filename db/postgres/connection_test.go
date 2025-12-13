package postgres

import (
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
				ConnectionString: "host=localhost port=5432 user=postgres password=postgres dbname=testdb sslmode=disable",
				ServiceName:      "test-service",
			},
			wantErr: false,
		},
		{
			name: "missing connection string",
			cfg: Config{
				ServiceName: "test-service",
			},
			wantErr: true,
		},
		{
			name: "tracing enabled without service name",
			cfg: Config{
				ConnectionString: "host=localhost port=5432",
				EnableTracing:    true,
			},
			wantErr: true,
		},
		{
			name: "max idle conns greater than max open conns",
			cfg: Config{
				ConnectionString: "host=localhost port=5432",
				MaxOpenConns:     50,
				MaxIdleConns:     100,
			},
			wantErr: true,
		},
		{
			name: "valid with tracing",
			cfg: Config{
				ConnectionString: "host=localhost port=5432",
				EnableTracing:    true,
				ServiceName:      "test-service",
			},
			wantErr: false,
		},
		{
			name: "valid with metrics",
			cfg: Config{
				ConnectionString: "host=localhost port=5432",
				EnableTracing:    true,
				EnableMetrics:    true,
				ServiceName:      "test-service",
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

func TestMaskConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		want    string
	}{
		{
			name:    "short connection string",
			connStr: "host=localhost",
			want:    "***",
		},
		{
			name:    "long connection string",
			connStr: "host=localhost port=5432 user=postgres password=secretpass dbname=mydb sslmode=disable",
			want:    "host=local***de=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskConnectionString(tt.connStr)
			if got != tt.want {
				t.Errorf("maskConnectionString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractDBName(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		want    string
	}{
		{
			name:    "standard format",
			connStr: "host=localhost port=5432 user=postgres password=pass dbname=mydb sslmode=disable",
			want:    "mydb",
		},
		{
			name:    "dbname at end",
			connStr: "host=localhost port=5432 user=postgres password=pass sslmode=disable dbname=testdb",
			want:    "testdb",
		},
		{
			name:    "no dbname",
			connStr: "host=localhost port=5432 user=postgres password=pass",
			want:    "postgres",
		},
		{
			name:    "dbname with special chars",
			connStr: "host=localhost dbname=my_test_db",
			want:    "my_test_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDBName(tt.connStr)
			if got != tt.want {
				t.Errorf("extractDBName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{
		ConnectionString: "host=localhost port=5432",
	}

	// Validate doesn't fail with missing optional fields
	if err := cfg.Validate(); err != nil {
		t.Errorf("Config.Validate() with defaults should not error: %v", err)
	}
}

func TestConfig_ValidatePoolSizes(t *testing.T) {
	tests := []struct {
		name         string
		maxOpenConns int
		maxIdleConns int
		wantErr      bool
	}{
		{
			name:         "idle equals open",
			maxOpenConns: 50,
			maxIdleConns: 50,
			wantErr:      false,
		},
		{
			name:         "idle less than open",
			maxOpenConns: 100,
			maxIdleConns: 10,
			wantErr:      false,
		},
		{
			name:         "both zero",
			maxOpenConns: 0,
			maxIdleConns: 0,
			wantErr:      false,
		},
		{
			name:         "only open set",
			maxOpenConns: 100,
			maxIdleConns: 0,
			wantErr:      false,
		},
		{
			name:         "idle greater than open",
			maxOpenConns: 50,
			maxIdleConns: 100,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				ConnectionString: "host=localhost port=5432",
				MaxOpenConns:     tt.maxOpenConns,
				MaxIdleConns:     tt.maxIdleConns,
			}

			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

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
				ConnectionString: "host=localhost port=5432",
				EnableTracing:    tt.enableTracing,
				ServiceName:      tt.serviceName,
			}

			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigWithTimeouts(t *testing.T) {
	cfg := Config{
		ConnectionString: "host=localhost port=5432",
		ConnMaxLifetime:  30 * time.Minute,
		ConnMaxIdleTime:  5 * time.Minute,
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Config with timeouts should be valid: %v", err)
	}
}

func TestConfigPrepareStmt(t *testing.T) {
	tests := []struct {
		name        string
		prepareStmt bool
	}{
		{
			name:        "prepare stmt enabled",
			prepareStmt: true,
		},
		{
			name:        "prepare stmt disabled",
			prepareStmt: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				ConnectionString: "host=localhost port=5432",
				PrepareStmt:      tt.prepareStmt,
			}

			if err := cfg.Validate(); err != nil {
				t.Errorf("Config with PrepareStmt=%v should be valid: %v", tt.prepareStmt, err)
			}
		})
	}
}

func TestColumnsToClause(t *testing.T) {
	columns := []string{"id", "email", "name"}
	result := columnsToClause(columns)

	if len(result) != len(columns) {
		t.Errorf("columnsToClause() returned %d columns, want %d", len(result), len(columns))
	}

	for i, col := range columns {
		if result[i].Name != col {
			t.Errorf("columnsToClause()[%d].Name = %v, want %v", i, result[i].Name, col)
		}
	}
}
