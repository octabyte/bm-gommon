package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

func Init(level string) {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(getLogLevelFromString(level)),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderCfg,
		OutputPaths: []string{
			"stdout",
		},
		ErrorOutputPaths: []string{
			"stderr",
		},
		InitialFields: map[string]interface{}{
			"pid": os.Getpid(),
		},
	}
	zap.ReplaceGlobals(zap.Must(config.Build()))
	zap.AddCallerSkip(1)
}

func LogDebug(msg string, fields ...zap.Field) {
	zap.L().Debug(msg, fields...)
}

func LogDebugf(msg string, args ...interface{}) {
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Debug(fmtdMsg)
}

func LogInfo(msg string, fields ...zap.Field) {
	zap.L().Info(msg, fields...)
}

func LogInfof(msg string, args ...interface{}) {
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Info(fmtdMsg)
}

func LogWarn(msg string, fields ...zap.Field) {
	zap.L().Warn(msg, fields...)
}

func LogWarnf(msg string, args ...interface{}) {
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Warn(fmtdMsg)
}

func LogError(msg string, fields ...zap.Field) {
	zap.L().Error(msg, fields...)
}

func LogErrorf(msg string, args ...interface{}) {
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Error(fmtdMsg)
}

func LogFatal(msg string, fields ...zap.Field) {
	zap.L().Fatal(msg, fields...)
}

func LogFatalf(msg string, args ...interface{}) {
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Fatal(fmtdMsg)
}

func LogPanic(msg string, fields ...zap.Field) {
	zap.L().Panic(msg, fields...)
}

func LogPanicf(msg string, args ...interface{}) {
	fmtdMsg := fmt.Sprintf(msg, args...)
	zap.L().Panic(fmtdMsg)
}

func getLogLevelFromString(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func Sync() {
	_ = zap.L().Sync()
}
