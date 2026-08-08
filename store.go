package taskqueue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisStore is the Redis-backed persistence layer for tasks.  It stores task
// state as hashes and manages the FIFO queue (task's id) used by consumers to
// pull the next task.
type RedisStore struct {
	rdb redis.UniversalClient
}

func NewRedisStore(rdb redis.UniversalClient) *RedisStore {
	return &RedisStore{rdb: rdb}
}

const (
	prefixKeyTask  = "taskqueue:task"
	prefixKeyQueue = "taskqueue:queue"
)

// Enqueue adds t to the task queue and stores its initial state.
// It sets the task's status to waiting and records the creation time.
// The task ID is pushed onto the queue atomically with the state.
// It returns an error if the operation fails.
func (s *RedisStore) Enqueue(ctx context.Context, t *Task) error {
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

// Dequeue removes t from the task queue.
func (s *RedisStore) Dequeue(ctx context.Context, t *Task) (taskID uuid.UUID, err error) {
	idString, err := s.rdb.BRPop(ctx, prefixKeyQueue).Result()
	if err != nil {
		return uuid.Nil, err
	}
	taskID, err = uuid.Parse(idString)
	if err != nil {
		return uuid.Nil, err
	}

	key := prefixKeyTask + taskID.String() // taskqueue:task:<id>

	// Read all hash fields and mark the status as completed in one pipeline.
	pipe := s.rdb.Pipeline()
	hgetallCmd := pipe.HGetAll(ctx, key)
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
	t.createdAt = time.UnixMilli(r.CreatedAt)
	if err := json.Unmarshal([]byte(r.Spec), &t.taskSpec); err != nil {
		return uuid.Nil, err
	}

	return taskID, nil
}

// UpdateStatus sets status of the task with taskID.
func (s *RedisStore) UpdateStatus(ctx context.Context, taskID uuid.UUID, status TaskStatus) error {
	key := prefixKeyTask + taskID.String()
	return s.rdb.HSet(ctx, key, "status", int(status)).Err()
}
