package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"time"

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
	MaxRetries     int             `json:"maxRetries,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Delay          time.Duration   `json:"delay,omitempty"` //延迟执行时间，默认立即（0）
	Timeout        time.Duration   `json:"timeout,omitempty"`
	ID             string          `json:"id,omitempty"`
	Status         string          `json:"status"`
	CreatedAt      time.Time       `json:"createdAt"`
	Result         string          `json:"result"`
}

// Options 是调用方创建任务时能提供的参数。
type Options struct {
	TaskType       string          `json:"taskType"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     *int            `json:"maxRetries,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Delay          time.Duration   `json:"delay,omitempty"` //延迟执行时间，默认立即（0）
	Timeout        *time.Duration  `json:"timeout,omitempty"`
}

// NewTask 纯构造：填默认值 + 系统字段（ID/Status/CreatedAt），
// 返回的 Task 一定是完整的，可以直接 Submit。
func NewTask(opts Options) *Task {
	maxRetries := 3
	if opts.MaxRetries != nil {
		maxRetries = *opts.MaxRetries
	}
	timeout := 30 * time.Second
	if opts.Timeout != nil {
		timeout = *opts.Timeout
	}

	return &Task{
		ID:             uuid.New().String(),
		TaskType:       opts.TaskType,
		Payload:        opts.Payload,
		MaxRetries:     maxRetries,
		IdempotencyKey: opts.IdempotencyKey,
		Delay:          opts.Delay,
		Timeout:        timeout,
		Status:         StatusWaiting,
		CreatedAt:      time.Now(),
	}
}

// Submit 校验并落库：把任务序列化后存入 "task:<id>"，再把 ID 推入队列。
func (t *Task) Submit(rdb *redis.Client) error {
	if t.TaskType == "" {
		return errors.New("task type is required")
	}
	if len(t.Payload) == 0 || string(t.Payload) == "null" {
		return errors.New("payload is required")
	}

	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	ctx := context.Background()
	key := fmt.Sprintf("task:%s", t.ID)
	// TODO: use lua script to ensure atomtic
	if err := rdb.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("store task: %w", err)
	}
	if err := rdb.LPush(ctx, taskQueueRedisKey, t.ID).Err(); err != nil {
		return fmt.Errorf("push to task queue: %w", err)
	}
	return nil
}

func getTask(rdb *redis.Client, id string) (*Task, error) {
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

func writeTask(ctx context.Context, rdb *redis.Client, task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	key := fmt.Sprintf("task:%s", task.ID)
	if err := rdb.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("store task: %w", err)
	}
	return nil
}
