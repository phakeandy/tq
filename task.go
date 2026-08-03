package tq

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
	MaxRetries     int             `json:"maxRetries,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Delay          time.Duration   `json:"delay,omitempty"`
	Timeout        time.Duration   `json:"timeout,omitempty"`
	ID             string          `json:"id,omitempty"`
	Status         string          `json:"status"`
	CreatedAt      time.Time       `json:"createdAt"`
	Result         string          `json:"result"`
}

// Options keeps the settings to build a new Task.
type Options struct {
	TaskType       string          `json:"taskType"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     *int            `json:"maxRetries,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Delay          time.Duration   `json:"delay,omitempty"`
	Timeout        *time.Duration  `json:"timeout,omitempty"`
}

// NewTask returns a new Task with default value.
// It check weather value from opts are illegal.
func NewTask(opts Options) (*Task, error) {
	if opts.TaskType == "" {
		return nil, errors.New("task type is required")
	}
	if len(opts.Payload) == 0 || string(opts.Payload) == "null" {
		return nil, errors.New("payload is required")
	}

	maxRetries := 3
	if opts.MaxRetries != nil {
		maxRetries = *opts.MaxRetries
	}
	timeout := 30 * time.Second
	if opts.Timeout != nil {
		timeout = *opts.Timeout
	}

	t := &Task{
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
	return t, nil
}

// Submit enqueues task to redis
func (t *Task) Submit(rdb *redis.Client) error {
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
