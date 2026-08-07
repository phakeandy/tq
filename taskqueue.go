package taskqueue

import (
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

// Option configures a Task created by NewTask. Options are applied in order,
// so a later option overrides an earlier one.
type Option func(*options)

// options holds the tunable settings of a Task.
type options struct {
	maxRetries     int
	idempotencyKey string
	delay          time.Duration
	timeout        time.Duration
}

// WithMaxRetries sets the maximum number of retries on failure.
// Not calling it uses the default (3). 0 means no retries at all.
// TODO(F4): retry logic not yet implemented; field is reserved.
func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = n }
}

// WithIdempotencyKey ensures tasks with the same key execute at most once.
// Empty string disables it.
// TODO(F5): idempotent delivery not yet implemented; field is reserved.
func WithIdempotencyKey(k string) Option {
	return func(o *options) { o.idempotencyKey = k }
}

// WithDelay postpones execution by this duration. Zero means immediate execution.
// TODO(F3): delayed execution not yet implemented; field is reserved.
func WithDelay(d time.Duration) Option {
	return func(o *options) { o.delay = d }
}

// WithTimeout sets the maximum allowed duration for a single execution attempt.
// Not calling it uses the default (30s). 0 means no timeout.
// TODO(F8): ctx is already passed to handler, but handlers ignoring ctx are not forcibly aborted.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// NewTask returns a new Task with default values.
// It checks whether the arguments are legal.
//
// taskType and payload are required; everything else is optional and can be
// configured with the With* options above.
func NewTask(taskType string, payload []byte, opts ...Option) (*Task, error) {
	o := options{
		maxRetries: 3,
		timeout:    30 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}

	info := &taskInfo{
		id:        uuid.New(),
		status:    StatusWaiting,
		createdAt: time.Now(),
	}

	return &Task{
		taskInfo: *info,
		taskSpec: taskSpec{
			Typename:       taskType,
			Payload:        payload,
			MaxRetries:     o.maxRetries,
			IdempotencyKey: o.idempotencyKey,
			Delay:          o.delay,
			Timeout:        o.timeout,
		},
	}, nil
}
