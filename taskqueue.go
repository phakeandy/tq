package taskqueue

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Task representes ...
type Task struct {
	taskInfo
	taskSpec
}

// TaskType holds the runtime infomation of a Task.
type taskInfo struct {
	id        uuid.UUID
	status    TaskStatus
	createdAt time.Time
	result    []byte
}

// TaskSpec holds the user's defination of a Task.
type taskSpec struct {
	Typename       string        `json:"typename"`
	Payload        []byte        `json:"payload"`
	MaxRetries     int           `json:"max_retries"`
	IdempotencyKey string        `json:"idempotency_key"`
	Delay          time.Duration `json:"delay"`
	Timeout        time.Duration `json:"timeout"`
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
		panic(fmt.Sprintf("ERROR: illegal status number: %d", s))
	}
}

var _ fmt.Stringer = StatusWaiting

type Consumer struct{}
type Producer struct{}

// Options keeps the settings to build a new Task.
type Options struct {
	// TaskType identifies which Worker handles this task (maps to RegisterHandler key). Required.
	TaskType string

	// Payload carries opaque business data. The engine does not interpret it. Required.
	Payload []byte

	// MaxRetries is the maximum number of retries on failure. nil means use default (3).
	// TODO(F4): retry logic not yet implemented; field is reserved.
	MaxRetries *int

	// IdempotencyKey ensures tasks with the same key execute at most once. Empty string disables it.
	// TODO(F5): idempotent delivery not yet implemented; field is reserved.
	IdempotencyKey string

	// Delay postpones execution by this duration. Zero means immediate execution.
	// TODO(F3): delayed execution not yet implemented; field is reserved.
	Delay time.Duration

	// Timeout is the maximum allowed duration for a single execution attempt. Zero means default (30s).
	// TODO(F8): ctx is already passed to handler, but handlers ignoring ctx are not forcibly aborted.
	Timeout time.Duration
}

// NewTask returns a new Task with default value.
// It check weather value from opts are illegal.
func NewTask(opts Options) (t *Task, err error) {
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
	if opts.Timeout != 0 {
		timeout = opts.Timeout
	}

	info := &taskInfo{
		id:        uuid.New(),
		status:    StatusWaiting,
		createdAt: time.Now(),
	}

	t := &Task{
		taskInfo: *info,
		taskSpec: taskSpec{
			Typename:       opts.TaskType,
			Payload:        opts.Payload,
			MaxRetries:     maxRetries,
			IdempotencyKey: opts.IdempotencyKey,
			Delay:          opts.Delay,
			Timeout:        timeout,
		},
	}
	return t, nil
}
