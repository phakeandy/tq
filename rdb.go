package tq

import (
	"os"

	"github.com/redis/go-redis/v9"
)

var (
	TASKQUEUE_REDIS_ADDRESS  = os.Getenv("TASKQUEUE_REDIS_ADDRESS")
	TASKQUEUE_REDIS_PASSWORD = os.Getenv("TASKQUEUE_REDIS_PASSWORD")
)

// NewRDB creates a Redis client using the TASKQUEUE_REDIS_ADDRESS and
// TASKQUEUE_REDIS_PASSWORD environment variables. If the address is not
// set, it defaults to localhost:6379.
func NewRDB() *redis.Client {
	addr := TASKQUEUE_REDIS_ADDRESS
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: TASKQUEUE_REDIS_PASSWORD,
		DB:       0, // use default DB
	})
	return rdb
}
