package tq

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Task represents a unit of work to be performed.
type Task struct {
	// typ indicates the type of task to be performed.
	typ string

	// payload holds data needed to perform the task.
	payload []byte

	// opts is the task's config.
	opts []Option
}

// NewTask creates a Task with the given options applied.
func NewTask(typ string, payload string, opts ...Option) *Task {
	return &Task{
		typ:     typ,
		payload: payload,
		opts:    opts,
	}
}

type TaskStatus int

const (
	StatusPending TaskStatus = iota
	StatusRunning
	StatusCompleted
	StatusFailed
)

// String returns the string representation of the task status.
func (s TaskStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
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

var _ fmt.Stringer = StatusPending

// Broker wraps a redis client and implements the task persistence layer.
type Broker struct {
	client redis.UniversalClient
}

func (b *Broker) enqueue(ctx context.Context, t *Task) error {
	panic("TODO")
}
func (b *Broker) dequeue(ctx context.Context) (id uuid.UUID, err error) {
	panic("TODO")
}
func (b *Broker) markAsCompleted(ctx context.Context, id uuid.UUID) error {
	panic("TODO")
}
func (b *Broker) markAsFailed(ctx context.Context, id uuid.UUID, reason string) error {
	panic("TODO")
}

// Option configures a Task created by NewTask.  A later option overrides an
// earlier one.
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

type Handle func(ctx context.Context, t *Task) error

type H map[string]Handle

func Run(ctx context.Context, broker Broker, handlemap H, concurrency int) error {
	// TODO
}
