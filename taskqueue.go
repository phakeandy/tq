package taskqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis"
)

// Task representes ...
type Task struct {
	TaskInfo
	TaskSpec
}

// TaskType holds the runtime infomation of a Task.
type TaskInfo struct {
	ID        uuid.UUID
	Status    TaskStatus
	CreatedAt time.Time
	Result    []byte
}

// TaskSpec holds the user's defination of a Task.
type TaskSpec struct {
	Type           string
	Payload        []byte
	MaxRetries     int
	IdempotencyKey string
	Delay          time.Duration
	Timeout        time.Duration
}

// Options keeps the settings to build a new Task.
type Options struct {
	TaskType       string
	Payload        []byte
	MaxRetries     *int
	IdempotencyKey string
	Delay          time.Duration
	Timeout        *time.Duration
}

// TaskStatus representes the Task' status.
type TaskStatus int

const (
	StatusWaiting TaskStatus = iota
	StatusRunning
	StatusCompleted
	StatusFailed
)

// String returns the string representation of the task status.
func (s TaskStatus) String() string {
	switch s {
	case StatusWaiting:
		return "waiting"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	default:
		panic(fmt.Sprintf("ERROR: illegal status number: %v", s))
	}
}

var _ fmt.Stringer = StatusWaiting

type Consumer struct{}
type Producer struct{}
