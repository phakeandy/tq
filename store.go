package taskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Storer ...
type Storer struct {
	rdb redis.UniversalClient
}

const (
	prefixKeyTask  = "taskqueue:task"
	prefixKeyQueue = "taskqueue:queue"
)

// Enqueue adds t to the task queue and stores its initial state.
// It sets the task's status to waiting and records the creation time.
// The task ID is pushed onto the queue atomically with the state.
// It returns an error if the operation fails.
func (s *Storer) Enqueue(ctx context.Context, t *Task) error {
	key := prefixKeyTask + t.id.String() // taskqueue:task:<id>
	specJSON, err := json.Marshal(t.taskSpec)
	if err != nil {
		return err
	}

	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key,
		"spec", specJSON,
		"status", int(StatusWaiting),
		"created_at", time.Now().UnixMilli(),
	)
	pipe.LPush(ctx, prefixKeyQueue, t.id.String())
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Dequeue removes t from the task queue and set its status to completed.
func (s *Storer) Dequeue(ctx context.Context, t *Task) (taskID uuid.UUID, err error) {
	idString, err := s.rdb.RPop(ctx, prefixKeyQueue).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return uuid.Nil, fmt.Errorf("task queue is empty: %w", err)
		}
		return uuid.Nil, err
	}
	taskID, err = uuid.Parse(idString)
	if err != nil {
		return uuid.Nil, err
	}

	key := prefixKeyTask + taskID.String() // taskqueue:task:<id>

	// 一次 pipeline：读完数据的同时把 status 设为 completed
	pipe := s.rdb.Pipeline()
	hgetallCmd := pipe.HGetAll(ctx, key)
	pipe.HSet(ctx, key, "status", int(StatusCompleted))
	if _, err := pipe.Exec(ctx); err != nil {
		return uuid.Nil, err
	}

	var r struct {
		Spec      string `redis:"spec"`
		Status    int    `redis:"status"`
		CreatedAt int64  `redis:"created_at"`
	}
	if err := hgetallCmd.Scan(&r); err != nil {
		return uuid.Nil, err
	}

	t.id = taskID
	t.status = StatusCompleted
	t.createdAt = time.UnixMilli(r.CreatedAt)
	if err := json.Unmarshal([]byte(r.Spec), &t.taskSpec); err != nil {
		return uuid.Nil, err
	}

	return taskID, nil
}
