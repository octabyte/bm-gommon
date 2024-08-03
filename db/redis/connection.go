package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
)

// Config holds the configuration for the Redis client
type Config struct {
	Addr     string
	Password string
	DB       int
}

func NewRedisClient(ctx context.Context, cfg Config) *redis.Client {
	options := &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := redis.NewClient(options)

	// Ping the Redis server to ensure the connection is established
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	return client
}

func Ping(ctx context.Context, client *redis.Client) error {
	return client.Ping(ctx).Err()
}
