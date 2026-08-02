package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RunWorker starts a pool of worker goroutines that pull tasks from Redis,
// execute the registered handler, and update task status.
func RunWorker(ctx context.Context, rdb *redis.Client, concurrency int) {
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			log.Printf("[worker-%d] started", workerID)
			for {
				select {
				case <-ctx.Done():
					log.Printf("[worker-%d] shutting down", workerID)
					return
				default:
				}

				// 1. 阻塞取任务
				result, err := rdb.BRPop(ctx, 0, "task_queue").Result()
				if err != nil {
					if ctx.Err() != nil {
						// context 被取消——这是正常的关闭
						return
					}
					log.Printf("[worker-%d] BRPop error: %v", workerID, err)
					time.Sleep(time.Second)
					continue
				}
				taskID := result[1] // BRPop returns [key, value]

				// 2. 获取任务数据
				task, err := GetTask(rdb, taskID)
				if err != nil {
					log.Printf("[worker-%d] get task %s: %v", workerID, taskID, err)
					continue
				}

				// 3. 查找 handler
				handlersMu.RLock()
				handler, ok := handlers[task.TaskType]
				handlersMu.RUnlock()
				if !ok {
					log.Printf("[worker-%d] no handler for task type %q", workerID, task.TaskType)
					continue
				}

				// 4. 标记 Running
				task.Status = StatusRunning
				log.Printf("[worker-%d] processing %s", workerID, task.ID)
				if err := updateTaskStatus(rdb, task); err != nil {
					log.Printf("[worker-%d] mark running %s: %v", workerID, task.ID, err)
				}

				// 5. 执行（带超时）
				taskCtx, cancel := context.WithTimeout(ctx, *task.Timeout)
				handlerErr := handler(taskCtx, task)
				cancel()

				// 6. 结果处理
				if handlerErr != nil {
					log.Printf("[worker-%d] task %s failed: %v", workerID, task.ID, handlerErr)
					task.Status = StatusFailed
				} else {
					task.Status = StatusCompleted
				}
				if err := updateTaskStatus(rdb, task); err != nil {
					log.Printf("[worker-%d] update status %s: %v", workerID, task.ID, err)
				}
			}
		}(i)
	}
	wg.Wait()
	log.Println("all workers stopped")
}

func updateTaskStatus(rdb *redis.Client, task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	key := fmt.Sprintf("task:%s", task.ID)
	return rdb.Set(context.Background(), key, data, 0).Err()
}
