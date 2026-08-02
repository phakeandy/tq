package main

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RunWorker starts a pool of worker goroutines that pull tasks from Redis,
// execute the registered handler, and update task status.
func RunWorker(ctx context.Context, rdb *redis.Client, concurrency int) {
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					rdb.BRPop(ctx, 0, taskQueueRedisKey)
				}
			}
		}()
	}

	wg.Wait()
}
