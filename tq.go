package tq

import (
	"context"
	"fmt"
	"sync"
	"time"
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
func NewTask(typ string, payload []byte, opts ...Option) *Task {
	return &Task{
		typ:     typ,
		payload: payload,
		opts:    opts,
	}
}

type Status int

const (
	StatusPending Status = iota
	StatusRunning
	StatusCompleted
	StatusFailed
)

// String returns the string representation of the status.
func (s Status) String() string {
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

// Handle executes a single dequeued job. Returning nil marks the job
// completed; returning an error marks it failed.
type Handle func(ctx context.Context, j *Job) error

type H map[string]Handle

// Run launches the main loop of the queue.
// It spawns concurrency worker goroutines that block on dequeue and dispatch
// each task to its registered handler.
//
// It blocks until ctx is cancelled.
func Run(ctx context.Context, rdb *RDB, handlemap H, concurrency int) error {
	if concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive, got %d", concurrency)
	}
	if handlemap == nil {
		return fmt.Errorf("handlemap must not be nil")
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				job, err := rdb.dequeue(ctx, defaultQueueName)
				if err != nil {
					continue
				}

				handler, ok := handlemap[job.Type]
				if !ok {
					// TODO: add retry
					_ = rdb.markAsFailed(ctx, job, "no handler for task type")
					continue
				}

				if err := handler(ctx, job); err != nil {
					// TODO: add retry
					_ = rdb.markAsFailed(ctx, job, err.Error())
				} else {
					_ = rdb.markAsCompleted(ctx, job)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}
