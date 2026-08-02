package main

import (
	"context"
	"fmt"
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
			fmt.Printf("work started\n")
			defer wg.Done()
		}()
	}

	wg.Wait()
}
