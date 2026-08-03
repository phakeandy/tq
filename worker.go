package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RunWorker starts a pool of worker goroutines that pull tasks from Redis,
// execute the registered handler, and update task status.
func RunWorker(ctx context.Context, rdb *redis.Client, concurrency int) {
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go work(ctx, &wg, rdb)
	}
	wg.Wait()
}

func work(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := rdb.BRPop(ctx, 0, taskQueueRedisKey).Result()
			if err != nil {
				log.Printf("BRPOP task queue: %v", err)
				continue
			}
			taskID := res[1]
			task, err := getTask(rdb, taskID)
			if err != nil {
				log.Printf("load task %s: %v", taskID, err)
				continue
			}
			task.Status = StatusRunning
			if err := writeTask(ctx, rdb, task); err != nil {
				log.Printf("write task %s: %v", taskID, err)
				continue

			}
		}
	}
}
