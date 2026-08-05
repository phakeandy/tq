package taskqueue

import (
	"os"

	"github.com/redis/go-redis/v9"
)

var (
	REDIS_ADDRESS  = os.Getenv("REDIS_ADDRESS")
	REDIS_PASSWORD = os.Getenv("REDIS_PASSWORD")
)

// NewRDB creates a Redis client using the TASKQUEUE_REDIS_ADDRESS and
// TASKQUEUE_REDIS_PASSWORD environment variables. If the address is not
// set, it defaults to localhost:6379.
func NewRDB() *redis.Client {
	addr := REDIS_ADDRESS
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: REDIS_PASSWORD,
		DB:       0, // use default DB
	})
	return rdb
}
