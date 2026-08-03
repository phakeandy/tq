package tq

import (
	"os"

	"github.com/redis/go-redis/v9"
)

var (
	TASKQUEUE_REDIS_ADDRESS  = os.Getenv("TASKQUEUE_REDIS_ADDRESS")
	TASKQUEUE_REDIS_PASSWORD = os.Getenv("TASKQUEUE_REDIS_PASSWORD")
)

func getRDB() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     TASKQUEUE_REDIS_ADDRESS,
		Password: TASKQUEUE_REDIS_PASSWORD,
		DB:       0, // use default DB
	})
	return rdb
}
