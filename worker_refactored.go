package tq

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- Public API (unchanged) ---

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

// --- polling loop ---

// work pulls tasks in a loop until ctx is cancelled.
//
// BRPop already respects context cancellation, so there's no need for an
// additional select wrapper.  We use a finite timeout (1 s) to avoid
// potentially very long blocking when the context expires between polls.
func work(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client) {
	defer wg.Done()

	for {
		res, err := rdb.BRPop(ctx, time.Second, taskQueueRedisKey).Result()
		if err != nil {
			if ctx.Err() != nil {
				return // graceful shutdown
			}
			slog.Error("BRPop task queue", "err", err)
			continue
		}
		taskID := res[1]
		processTask(ctx, rdb, taskID)
	}
}

// --- task lifecycle (the explicit state machine) ---

// processTask runs the full lifecycle of a single task:
//
//	StatusWaiting ──(load)──► StatusRunning ──(execute)──► StatusCompleted
//	                                                      StatusFailed
//
// Every path writes the final state back to Redis so the task is never left
// in an inconsistent state.
func processTask(ctx context.Context, rdb *redis.Client, taskID string) {
	// -------------------- Load & transition to Running --------------------
	task, err := GetTask(rdb, taskID)
	if err != nil {
		slog.Error("load task", "taskID", taskID, "err", err)
		return
	}

	task.Status = StatusRunning
	if !saveTask(ctx, rdb, task) {
		return
	}

	// -------------------- Look up handler --------------------
	handlersMu.RLock()
	handler, ok := handlers[task.TaskType]
	handlersMu.RUnlock()
	if !ok {
		slog.Error("no handler for task type", "taskType", task.TaskType)
		task.Status = StatusFailed // don't leave it running forever
		saveTask(ctx, rdb, task)
		return
	}

	// -------------------- Execute --------------------
	taskCtx, cancel := context.WithTimeout(ctx, task.Timeout)
	defer cancel()

	err = handler(taskCtx, task)
	if err != nil {
		task.Status = StatusFailed
	} else {
		task.Status = StatusCompleted
	}
	saveTask(ctx, rdb, task)
}

// saveTask persists the task and logs any error.
// It returns false when persistence failed (so callers can bail out).
func saveTask(ctx context.Context, rdb *redis.Client, task *Task) bool {
	if err := writeTask(ctx, rdb, task); err != nil {
		slog.Error("write task", "taskID", task.ID, "err", err)
		return false
	}
	return true
}
