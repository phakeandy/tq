package taskqueue

import (
	"context"
	"log/slog"
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
				if ctx.Err() != nil {
					return
				}
				slog.Error("BRPOP task queue", "err", err)
				continue
			}
			taskID := res[1]
			task, err := GetTask(rdb, taskID)
			if err != nil {
				slog.Error("load task", "taskID", taskID, "err", err)
				continue
			}
			task.Status = StatusRunning
			if err := writeTask(ctx, rdb, task); err != nil {
				slog.Error("write task", "taskID", taskID, "err", err)
				continue
			}

			handlersMu.RLock()
			handler, ok := handlers[task.TaskType]
			handlersMu.RUnlock()
			if !ok {
				slog.Error("no handler for task type", "taskType", task.TaskType)
				continue
			}

			taskCtx, cancel := context.WithTimeout(ctx, task.Timeout)
			err = handler(taskCtx, task)
			cancel()
			if err != nil {
				task.Status = StatusFailed
				if err := writeTask(ctx, rdb, task); err != nil {
					slog.Error("write task", "taskID", taskID, "err", err)
					continue
				}
				continue
			}
			task.Status = StatusCompleted
			if err := writeTask(ctx, rdb, task); err != nil {
				slog.Error("write task", "taskID", taskID, "err", err)
				continue
			}
		}
	}
}
