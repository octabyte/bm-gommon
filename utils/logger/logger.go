package logger

import (
	"fmt"
	"log"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	Level       string
	Env         string
	ServiceName string
}

func Init(cfg *Config) {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	// Ensure level is properly encoded for Railway
	encoderCfg.LevelKey = "level"
	encoderCfg.EncodeLevel = zapcore.LowercaseLevelEncoder

	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(getLogLevelFromString(cfg.Level)),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderCfg,
		// Send ALL logs to stdout to prevent Railway from misclassifying based on stream
		OutputPaths: []string{
			"stdout",
		},
		ErrorOutputPaths: []string{
			"stdout", // Changed from stderr to stdout
		},
		InitialFields: map[string]any{
			"pid":     os.Getpid(),
			"env":     cfg.Env,
			"service": cfg.ServiceName,
		},
	}

	logger, err := config.Build()
	if err != nil {
		// Fallback to a basic logger if zap initialization fails
		log.Printf("Failed to initialize zap logger: %v. Using basic logger.", err)
		return
	}
	logger = logger.WithOptions(zap.AddCallerSkip(1))

	zap.ReplaceGlobals(logger)
}

func LogDebug(msg string, fields ...zap.Field) {
	zap.L().Debug(msg, fields...)
}

func LogDebugf(msg string, args ...any) {
	if len(args) == 0 {
		zap.L().Debug(msg)
		return
	}
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Debug(fmtdMsg)
}

func LogInfo(msg string, fields ...zap.Field) {
	zap.L().Info(msg, fields...)
}

func LogInfof(msg string, args ...any) {
	if len(args) == 0 {
		zap.L().Info(msg)
		return
	}
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Info(fmtdMsg)
}

func LogWarn(msg string, fields ...zap.Field) {
	zap.L().Warn(msg, fields...)
}

func LogWarnf(msg string, args ...any) {
	if len(args) == 0 {
		zap.L().Warn(msg)
		return
	}
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Warn(fmtdMsg)
}

func LogError(msg string, fields ...zap.Field) {
	zap.L().Error(msg, fields...)
}

func LogErrorf(msg string, args ...any) {
	if len(args) == 0 {
		zap.L().Error(msg)
		return
	}
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Error(fmtdMsg)
}

func LogFatal(msg string, fields ...zap.Field) {
	zap.L().Fatal(msg, fields...)
}

func LogFatalf(msg string, args ...any) {
	if len(args) == 0 {
		zap.L().Fatal(msg)
		return
	}
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Fatal(fmtdMsg)
}

func LogPanic(msg string, fields ...zap.Field) {
	zap.L().Panic(msg, fields...)
}

func LogPanicf(msg string, args ...any) {
	if len(args) == 0 {
		zap.L().Panic(msg)
		return
	}
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Panic(fmtdMsg)
}

func getLogLevelFromString(level string) zapcore.Level {
	// Make level parsing case-insensitive and handle common variations
	level = strings.ToLower(strings.TrimSpace(level))
	switch level {
	case "debug", "dbg":
		return zapcore.DebugLevel
	case "info", "information":
		return zapcore.InfoLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error", "err":
		return zapcore.ErrorLevel
	case "fatal":
		return zapcore.FatalLevel
	case "panic":
		return zapcore.PanicLevel
	default:
		return zapcore.InfoLevel
	}
}

func Sync() {
	if err := zap.L().Sync(); err != nil {
		// Only log sync errors that aren't related to stdout/stderr on some systems
		// This prevents noise from expected sync failures on stdout/stderr
		if !strings.Contains(err.Error(), "sync /dev/stdout") &&
			!strings.Contains(err.Error(), "sync /dev/stderr") {
			log.Printf("Failed to sync logger: %v", err)
		}
	}
}
