// Package testutil provides Redis-level test helpers shared across tq test files.
// It does not import the tq package — seed helpers that need tq internals stay
// in each _test.go file.
package testutil

import (
	"context"
	"flag"
	"testing"

	"github.com/redis/go-redis/v9"
)

var (
	RedisAddr string
	RedisDB   int
)

func init() {
	flag.StringVar(&RedisAddr, "redis_addr", "localhost:6379", "redis address to use in testing")
	flag.IntVar(&RedisDB, "redis_db", 15, "redis db number to use in testing")
}

// SetupRedis connects to a dedicated test DB, flushes it, and registers a
// Cleanup that closes the connection when the test finishes.
func SetupRedis(tb testing.TB) redis.UniversalClient {
	tb.Helper()
	client := redis.NewClient(&redis.Options{Addr: RedisAddr, DB: RedisDB})
	tb.Cleanup(func() {
		client.Close()
	})
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		tb.Fatalf("FlushDB: %v", err)
	}
	return client
}

// FlushDB deletes every key in the current database.
func FlushDB(tb testing.TB, client redis.UniversalClient) {
	tb.Helper()
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		tb.Fatalf("FlushDB: %v", err)
	}
}

// ──────────────────────────── read-back helpers ────────────────────────────

// GetHash returns all field-value pairs stored under key.
func GetHash(tb testing.TB, client redis.UniversalClient, key string) map[string]string {
	tb.Helper()
	fields, err := client.HGetAll(context.Background(), key).Result()
	if err != nil {
		tb.Fatalf("HGetAll %s: %v", key, err)
	}
	return fields
}

// GetList returns every member of a Redis list.
func GetList(tb testing.TB, client redis.UniversalClient, key string) []string {
	tb.Helper()
	vals, err := client.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		tb.Fatalf("LRange %s: %v", key, err)
	}
	return vals
}

// GetZSet returns every member (with its score) of a Redis sorted set.
func GetZSet(tb testing.TB, client redis.UniversalClient, key string) []redis.Z {
	tb.Helper()
	zs, err := client.ZRangeWithScores(context.Background(), key, 0, -1).Result()
	if err != nil {
		tb.Fatalf("ZRangeWithScores %s: %v", key, err)
	}
	return zs
}
