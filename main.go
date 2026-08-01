package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	taskStore = make(map[string]*Task)
	mu        sync.Mutex
)

type TaskStatus int

const (
	Waiting TaskStatus = iota
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

type SubmitRequest struct {
	TaskType       string          `json:"taskType"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     *int            `json:"maxRetries,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Delay          time.Duration   `json:"delay,omitempty"` //延迟执行时间，默认立即（0）
	Timeout        *time.Duration  `json:"timeout,omitempty"`
}

func Submit(req SubmitRequest) (id string, err error) {
	if req.TaskType == "" {
		err = errors.New("task type is required")
		return
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		err = errors.New("payload is required")
		return
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
		Status:         "waiting",
		CreatedAt:      time.Now(),
	}

	mu.Lock()
	taskStore[id] = task
	mu.Unlock()

	return
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := rdb.Get(ctx, "key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	id, err := Submit(SubmitRequest{
		TaskType: "hello",
		Payload:  []byte(`"balabala"`),
	})
	fmt.Println(id, err)
}
