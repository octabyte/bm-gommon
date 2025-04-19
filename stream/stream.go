package stream

import (
	"time"

	"github.com/octabyte/bm-gommon/utils/logger"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type StreamConfig struct {
	StreamName     string
	MaxSegmentSize int64
	MaxSegmentAge  int64
	MaxAge         time.Duration
}

func DeclareStream(environment *stream.Environment, streamConfig StreamConfig) {
	if environment == nil {
		logger.LogFatalf("Environment is nil")
	}

	if streamConfig.StreamName == "" {
		logger.LogFatalf("Stream name is required")
	}

	if streamConfig.MaxSegmentSize == 0 {
		streamConfig.MaxSegmentSize = 1024 * 1024 * 10 // 10MB
	}

	if streamConfig.MaxSegmentAge == 0 {
		streamConfig.MaxAge = 10 * time.Minute
	}

	err := environment.DeclareStream(streamConfig.StreamName, stream.NewStreamOptions().
		SetMaxSegmentSizeBytes(stream.ByteCapacity{}.MB(streamConfig.MaxSegmentSize)).
		SetMaxLengthBytes(stream.ByteCapacity{}.MB(streamConfig.MaxSegmentSize)).
		SetMaxAge(streamConfig.MaxAge))

	if err != nil {
		logger.LogFatalf("Failed to declare stream: %v", err)
	}
}
