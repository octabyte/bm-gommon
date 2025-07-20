package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Set sets a key-value pair in Redis.
func Set(ctx context.Context, client *redis.Client, key string, value interface{}, ttl time.Duration) error {
	return client.Set(ctx, key, value, ttl).Err()
}

// Get retrieves the value of a key from Redis.
func Get(ctx context.Context, client *redis.Client, key string) (string, error) {
	return client.Get(ctx, key).Result()
}

// Del deletes a key from Redis.
func Del(ctx context.Context, client *redis.Client, key string) error {
	return client.Del(ctx, key).Err()
}

// Exists checks if a key exists in Redis.
func Exists(ctx context.Context, client *redis.Client, key string) (bool, error) {
	exists, err := client.Exists(ctx, key).Result()
	return exists > 0, err
}

// Expire sets an expiration time for a key in Redis.
func Expire(ctx context.Context, client *redis.Client, key string, expiration time.Duration) error {
	return client.Expire(ctx, key, expiration).Err()
}

// Keys retrieves all keys matching a pattern from Redis.
func Keys(ctx context.Context, client *redis.Client, pattern string) ([]string, error) {
	return client.Keys(ctx, pattern).Result()
}

// HSet sets a field in a hash in Redis.
func HSet(ctx context.Context, client *redis.Client, key, field string, value interface{}) error {
	return client.HSet(ctx, key, field, value).Err()
}

// HGet retrieves a field from a hash in Redis.
func HGet(ctx context.Context, client *redis.Client, key, field string) (string, error) {
	return client.HGet(ctx, key, field).Result()
}

// HDel deletes a field from a hash in Redis.
func HDel(ctx context.Context, client *redis.Client, key string, fields ...string) error {
	return client.HDel(ctx, key, fields...).Err()
}

// LPush pushes a value onto a list in Redis.
func LPush(ctx context.Context, client *redis.Client, key string, values ...interface{}) error {
	return client.LPush(ctx, key, values...).Err()
}

// RPop pops a value from the end of a list in Redis.
func RPop(ctx context.Context, client *redis.Client, key string) (string, error) {
	return client.RPop(ctx, key).Result()
}

// DelMany deletes multiple keys from Redis.
func DelMany(ctx context.Context, client *redis.Client, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return client.Del(ctx, keys...).Err()
}

// SetMany sets multiple key-value pairs in Redis.
func SetMany(ctx context.Context, client *redis.Client, pairs map[string]interface{}) error {
	if len(pairs) == 0 {
		return nil
	}

	// Convert map to slice of interface{} for MSet
	args := make([]interface{}, 0, len(pairs)*2)
	for key, value := range pairs {
		args = append(args, key, value)
	}

	return client.MSet(ctx, args...).Err()
}
