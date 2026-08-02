package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"github.com/google/uuid"

	"github.com/redis/go-redis/v9"
)

const (
	StatusWaiting   = "waiting"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

type Task struct {
	TaskType       string          `json:"taskType"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     *int            `json:"maxRetries,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Delay          time.Duration   `json:"delay,omitempty"` //延迟执行时间，默认立即（0）
	Timeout        *time.Duration  `json:"timeout,omitempty"`
	ID             string          `json:"id,omitempty"`
	Status         string          `json:"status"`
	CreatedAt      time.Time       `json:"createdAt"`
	Result         string          `json:"result"`
}

// SubmitRequest is a DTO-like struct for Task.
type SubmitRequest struct {
	TaskType       string          `json:"taskType"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     *int            `json:"maxRetries,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Delay          time.Duration   `json:"delay,omitempty"` //延迟执行时间，默认立即（0）
	Timeout        *time.Duration  `json:"timeout,omitempty"`
}

// Submit enqueues a new task into Redis and returns its unique ID.
// It validates the request, assigns default values if unset and stores the
// serialized task in Redis under "task:<id>".
func Submit(rdb *redis.Client, req SubmitRequest) (id string, err error) {
	if req.TaskType == "" {
		return "", errors.New("task type is required")
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		return "", errors.New("payload is required")
	}

	if req.MaxRetries == nil {
		defaultRetries := 3
		req.MaxRetries = &defaultRetries
	}
	if req.Timeout == nil {
		defaultTimeout := 30 * time.Second
		req.Timeout = &defaultTimeout
	}

	id = uuid.New().String()

	task := &Task{
		ID:             id,
		TaskType:       req.TaskType,
		Payload:        req.Payload,
		MaxRetries:     req.MaxRetries,
		IdempotencyKey: req.IdempotencyKey,
		Delay:          req.Delay,
		Timeout:        req.Timeout,
		Status:         StatusWaiting,
		CreatedAt:      time.Now(),
	}

	data, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("marshal task: %w", err)
	}

	ctx := context.Background()
	key := fmt.Sprintf("task:%s", id)
	if err := rdb.Set(ctx, key, data, 0).Err(); err != nil {
		return "", fmt.Errorf("store task: %w", err)
	}

	if err := rdb.LPush(ctx, "task_queue", id).Err(); err != nil {
		return "", fmt.Errorf("push to task_queue: %w", err)
	}
	return id, err
}

func GetTask(rdb *redis.Client, id string) (*Task, error) {
	key := fmt.Sprintf("task:%s", id)
	ctx := context.TODO()
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("task not found: %s", id)
		}
		return nil, err
	}
	var task Task
	if err := json.Unmarshal([]byte(val), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task data in redis: %w", err)
	}
	return &task, nil
}
