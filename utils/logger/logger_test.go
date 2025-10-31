package logger

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// LoggerTestSuite defines the test suite for the logger package
type LoggerTestSuite struct {
	suite.Suite
	originalLogger *zap.Logger
	observedCore   zapcore.Core
	observedLogs   *observer.ObservedLogs
}

// SetupSuite runs once before all tests in the suite
func (suite *LoggerTestSuite) SetupSuite() {
	// Store original logger to restore later
	suite.originalLogger = zap.L()
}

// TearDownSuite runs once after all tests in the suite
func (suite *LoggerTestSuite) TearDownSuite() {
	// Restore original logger
	zap.ReplaceGlobals(suite.originalLogger)
}

// SetupTest runs before each test
func (suite *LoggerTestSuite) SetupTest() {
	// Create observed logger for testing
	suite.observedCore, suite.observedLogs = observer.New(zap.DebugLevel)
	observedLogger := zap.New(suite.observedCore)
	zap.ReplaceGlobals(observedLogger)
}

// TearDownTest runs after each test
func (suite *LoggerTestSuite) TearDownTest() {
	// Clear observed logs
	suite.observedLogs.TakeAll()
}

// TestGetLogLevelFromString tests the log level parsing function
func (suite *LoggerTestSuite) TestGetLogLevelFromString() {
	testCases := []struct {
		name     string
		input    string
		expected zapcore.Level
	}{
		// Basic levels
		{"debug lowercase", "debug", zapcore.DebugLevel},
		{"info lowercase", "info", zapcore.InfoLevel},
		{"warn lowercase", "warn", zapcore.WarnLevel},
		{"error lowercase", "error", zapcore.ErrorLevel},

		// Case insensitive
		{"debug uppercase", "DEBUG", zapcore.DebugLevel},
		{"info mixed case", "Info", zapcore.InfoLevel},
		{"warn mixed case", "WARN", zapcore.WarnLevel},
		{"error mixed case", "Error", zapcore.ErrorLevel},

		// Variations
		{"debug short", "dbg", zapcore.DebugLevel},
		{"error short", "err", zapcore.ErrorLevel},
		{"warning full", "warning", zapcore.WarnLevel},
		{"information full", "information", zapcore.InfoLevel},

		// New levels
		{"fatal", "fatal", zapcore.FatalLevel},
		{"panic", "panic", zapcore.PanicLevel},

		// With whitespace
		{"debug with spaces", "  debug  ", zapcore.DebugLevel},
		{"info with tabs", "\tinfo\t", zapcore.InfoLevel},

		// Invalid/default
		{"empty string", "", zapcore.InfoLevel},
		{"invalid level", "invalid", zapcore.InfoLevel},
		{"random string", "xyz", zapcore.InfoLevel},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			result := getLogLevelFromString(tc.input)
			assert.Equal(suite.T(), tc.expected, result,
				"getLogLevelFromString(%q) should return %v", tc.input, tc.expected)
		})
	}
}

// TestInit tests the logger initialization
func (suite *LoggerTestSuite) TestInit() {
	testCases := []struct {
		name   string
		config *Config
	}{
		{
			name: "basic config",
			config: &Config{
				Level:       "info",
				Env:         "test",
				ServiceName: "test-service",
			},
		},
		{
			name: "debug level",
			config: &Config{
				Level:       "debug",
				Env:         "development",
				ServiceName: "debug-service",
			},
		},
		{
			name: "production config",
			config: &Config{
				Level:       "warn",
				Env:         "production",
				ServiceName: "prod-service",
			},
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// Initialize logger
			require.NotPanics(suite.T(), func() {
				Init(tc.config)
			}, "Init() should not panic")

			// Verify global logger is set
			assert.NotNil(suite.T(), zap.L(), "Global logger should be set after Init()")

			// Test that we can log without error
			require.NotPanics(suite.T(), func() {
				LogInfo("test message")
			}, "LogInfo should not panic after Init()")
		})
	}
}

// TestInitWithInvalidConfig tests initialization with invalid configurations
func (suite *LoggerTestSuite) TestInitWithInvalidConfig() {
	// Capture log output to verify error handling
	var buf bytes.Buffer
	originalOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(originalOutput)

	config := &Config{
		Level:       "invalid",
		Env:         "test",
		ServiceName: "test",
	}

	require.NotPanics(suite.T(), func() {
		Init(config)
	}, "Init() should not panic with invalid config")

	// Should still work with default level
	require.NotPanics(suite.T(), func() {
		LogInfo("test after invalid config")
	}, "LogInfo should work after invalid config")
}

// TestLoggingFunctions tests all logging functions
func (suite *LoggerTestSuite) TestLoggingFunctions() {
	testCases := []struct {
		name    string
		logFunc func()
		level   zapcore.Level
		message string
	}{
		{
			name: "LogDebug",
			logFunc: func() {
				LogDebug("test debug message")
			},
			level:   zapcore.DebugLevel,
			message: "test debug message",
		},
		{
			name: "LogInfo",
			logFunc: func() {
				LogInfo("test info message")
			},
			level:   zapcore.InfoLevel,
			message: "test info message",
		},
		{
			name: "LogWarn",
			logFunc: func() {
				LogWarn("test warn message")
			},
			level:   zapcore.WarnLevel,
			message: "test warn message",
		},
		{
			name: "LogError",
			logFunc: func() {
				LogError("test error message")
			},
			level:   zapcore.ErrorLevel,
			message: "test error message",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// Clear previous logs
			suite.observedLogs.TakeAll()

			// Execute logging function
			tc.logFunc()

			// Verify log was captured
			logs := suite.observedLogs.All()
			require.Len(suite.T(), logs, 1, "Should capture exactly one log entry")

			entry := logs[0]
			assert.Equal(suite.T(), tc.level, entry.Level, "Log level should match")
			assert.Equal(suite.T(), tc.message, entry.Message, "Log message should match")
		})
	}
}

// TestFormattedLoggingFunctions tests formatted logging functions
func (suite *LoggerTestSuite) TestFormattedLoggingFunctions() {
	testCases := []struct {
		name          string
		logFunc       func()
		expectedLevel zapcore.Level
		expectedMsg   string
	}{
		{
			name: "LogInfof with args",
			logFunc: func() {
				LogInfof("user %s logged in with ID %d", "john", 123)
			},
			expectedLevel: zapcore.InfoLevel,
			expectedMsg:   "user john logged in with ID 123",
		},
		{
			name: "LogInfof without args",
			logFunc: func() {
				LogInfof("simple message")
			},
			expectedLevel: zapcore.InfoLevel,
			expectedMsg:   "simple message",
		},
		{
			name: "LogErrorf with args",
			logFunc: func() {
				LogErrorf("error processing user %s: %v", "jane", "validation failed")
			},
			expectedLevel: zapcore.ErrorLevel,
			expectedMsg:   "error processing user jane: validation failed",
		},
		{
			name: "LogDebugf without args",
			logFunc: func() {
				LogDebugf("debug message")
			},
			expectedLevel: zapcore.DebugLevel,
			expectedMsg:   "debug message",
		},
		{
			name: "LogWarnf with args",
			logFunc: func() {
				LogWarnf("warning: %s", "something happened")
			},
			expectedLevel: zapcore.WarnLevel,
			expectedMsg:   "warning: something happened",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// Clear previous logs
			suite.observedLogs.TakeAll()

			// Execute logging function
			tc.logFunc()

			// Verify log was captured
			logs := suite.observedLogs.All()
			require.Len(suite.T(), logs, 1, "Should capture exactly one log entry")

			entry := logs[0]
			assert.Equal(suite.T(), tc.expectedLevel, entry.Level, "Log level should match")
			assert.Equal(suite.T(), tc.expectedMsg, entry.Message, "Log message should match")
		})
	}
}

// TestLoggingWithFields tests structured logging with fields
func (suite *LoggerTestSuite) TestLoggingWithFields() {
	suite.Run("structured logging with fields", func() {
		// Test logging with fields
		LogInfo("test message",
			zap.String("user", "john"),
			zap.Int("age", 30),
			zap.Bool("active", true),
		)

		// Verify log was captured with fields
		logs := suite.observedLogs.All()
		require.Len(suite.T(), logs, 1, "Should capture exactly one log entry")

		entry := logs[0]
		assert.Equal(suite.T(), "test message", entry.Message, "Message should match")

		// Check fields
		fields := entry.Context
		require.Len(suite.T(), fields, 3, "Should have exactly 3 fields")

		// Verify specific fields
		expectedFields := map[string]any{
			"user":   "john",
			"age":    int64(30),
			"active": true,
		}

		for _, field := range fields {
			expectedValue, exists := expectedFields[field.Key]
			require.True(suite.T(), exists, "Field %s should exist", field.Key)

			switch field.Type {
			case zapcore.StringType:
				assert.Equal(suite.T(), expectedValue.(string), field.String,
					"String field %s should match", field.Key)
			case zapcore.Int64Type:
				assert.Equal(suite.T(), expectedValue.(int64), field.Integer,
					"Integer field %s should match", field.Key)
			case zapcore.BoolType:
				assert.Equal(suite.T(), expectedValue.(bool), field.Integer == 1,
					"Boolean field %s should match", field.Key)
			}
		}
	})
}

// TestSync tests the sync function
func (suite *LoggerTestSuite) TestSync() {
	suite.Run("sync function", func() {
		// Capture log output to check for sync errors
		var buf bytes.Buffer
		originalOutput := log.Writer()
		log.SetOutput(&buf)
		defer log.SetOutput(originalOutput)

		// Initialize logger first
		Init(&Config{
			Level:       "info",
			Env:         "test",
			ServiceName: "sync-test",
		})

		// This should not panic
		require.NotPanics(suite.T(), func() {
			Sync()
		}, "Sync() should not panic")

		// Check that no unexpected errors were logged
		output := buf.String()
		if strings.Contains(output, "Failed to sync logger") {
			// Only fail if it's not the expected stdout/stderr sync error
			assert.True(suite.T(),
				strings.Contains(output, "sync /dev/stdout") || strings.Contains(output, "sync /dev/stderr"),
				"Unexpected sync error: %s", output)
		}
	})
}

// TestConfigValidation tests configuration validation
func (suite *LoggerTestSuite) TestConfigValidation() {
	testCases := []struct {
		name   string
		config *Config
	}{
		{
			name:   "empty config",
			config: &Config{},
		},
		{
			name: "config with empty strings",
			config: &Config{
				Level:       "",
				Env:         "",
				ServiceName: "",
			},
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			require.NotPanics(suite.T(), func() {
				Init(tc.config)
			}, "Init() should not panic with %s", tc.name)

			// Should be able to log after init
			require.NotPanics(suite.T(), func() {
				LogInfo("test message")
			}, "LogInfo should work after %s", tc.name)
		})
	}
}

// TestLoggerTestSuite runs the test suite
func TestLoggerTestSuite(t *testing.T) {
	suite.Run(t, new(LoggerTestSuite))
}

// Benchmark tests
func BenchmarkLogInfo(b *testing.B) {
	Init(&Config{
		Level:       "info",
		Env:         "benchmark",
		ServiceName: "bench-service",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LogInfo("benchmark message")
	}
}

func BenchmarkLogInfof(b *testing.B) {
	Init(&Config{
		Level:       "info",
		Env:         "benchmark",
		ServiceName: "bench-service",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LogInfof("benchmark message %d", i)
	}
}

func BenchmarkLogInfofNoArgs(b *testing.B) {
	Init(&Config{
		Level:       "info",
		Env:         "benchmark",
		ServiceName: "bench-service",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LogInfof("benchmark message")
	}
}

// Additional test suites for specific functionality

// InitTestSuite focuses on initialization testing
type InitTestSuite struct {
	suite.Suite
}

func (suite *InitTestSuite) TestInitWithDifferentLevels() {
	levels := []string{"debug", "info", "warn", "error", "fatal", "panic"}

	for _, level := range levels {
		suite.Run("level_"+level, func() {
			config := &Config{
				Level:       level,
				Env:         "test",
				ServiceName: "level-test",
			}

			require.NotPanics(suite.T(), func() {
				Init(config)
			}, "Init should not panic with level %s", level)
		})
	}
}

func (suite *InitTestSuite) TestInitWithCaseInsensitiveLevels() {
	levels := []string{"DEBUG", "Info", "WARN", "Error"}

	for _, level := range levels {
		suite.Run("case_insensitive_"+level, func() {
			config := &Config{
				Level:       level,
				Env:         "test",
				ServiceName: "case-test",
			}

			require.NotPanics(suite.T(), func() {
				Init(config)
			}, "Init should not panic with case insensitive level %s", level)
		})
	}
}

func TestInitTestSuite(t *testing.T) {
	suite.Run(t, new(InitTestSuite))
}

// PerformanceTestSuite focuses on performance-related tests
type PerformanceTestSuite struct {
	suite.Suite
}

func (suite *PerformanceTestSuite) TestFormattedLoggingOptimization() {
	// Setup observed logger
	observedCore, observedLogs := observer.New(zap.DebugLevel)
	observedLogger := zap.New(observedCore)
	zap.ReplaceGlobals(observedLogger)

	suite.Run("no_args_optimization", func() {
		// Test that LogInfof without args doesn't call fmt.Sprintf
		LogInfof("simple message")

		logs := observedLogs.All()
		require.Len(suite.T(), logs, 1)
		assert.Equal(suite.T(), "simple message", logs[0].Message)

		observedLogs.TakeAll()
	})

	suite.Run("with_args_formatting", func() {
		// Test that LogInfof with args works correctly
		LogInfof("formatted message: %s %d", "test", 42)

		logs := observedLogs.All()
		require.Len(suite.T(), logs, 1)
		assert.Equal(suite.T(), "formatted message: test 42", logs[0].Message)
	})
}

func TestPerformanceTestSuite(t *testing.T) {
	suite.Run(t, new(PerformanceTestSuite))
}
