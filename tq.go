package tq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	StatusScheduled
	StatusRetry
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
	case StatusScheduled:
		return "scheduled"
	case StatusRetry:
		return "retry"
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
	MaxRetries     int           `json:"max_retries"`
	IdempotencyKey string        `json:"idempotency_key"`
	Delay          time.Duration `json:"delay"`
	Timeout        time.Duration `json:"timeout"`
}

// defaultOptions returns the option values used when the corresponding
// With* function is not called. It must be applied BEFORE user options so
// that an explicit zero value (e.g. WithMaxRetries(0)) overrides a default.
func defaultOptions() options {
	return options{
		MaxRetries: 3,
		Timeout:    30 * time.Second,
	}
}

// WithMaxRetries sets the maximum number of retries on failure.
// Not calling it uses the default (3). 0 means no retries at all.
func WithMaxRetries(n int) Option {
	return func(o *options) { o.MaxRetries = n }
}

// WithIdempotencyKey ensures tasks with the same key execute at most once.
// Empty string disables it.
// TODO(F5): idempotent delivery not yet implemented; field is reserved.
func WithIdempotencyKey(k string) Option {
	return func(o *options) { o.IdempotencyKey = k }
}

// WithDelay postpones execution by this duration. Zero means immediate execution.
func WithDelay(d time.Duration) Option {
	return func(o *options) { o.Delay = d }
}

// WithTimeout sets the maximum allowed duration for a single execution attempt.
// Not calling it uses the default (30s). 0 means no timeout.
// TODO(F8): ctx is already passed to handler, but handlers ignoring ctx are not forcibly aborted.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.Timeout = d }
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
	wg.Add(concurrency + 2)
	for i := 0; i < concurrency; i++ {
		go workerLoop(ctx, &wg, rdb, handlemap)
	}
	go forwardLoop(ctx, &wg, rdb)
	go recoveryLoop(ctx, &wg, rdb)
	wg.Wait()
	return nil
}

var errNoHandler = errors.New("no handler for task type")

// runJob runs one job's handler with the job's timeout policy.
// Panics from the handler are converted to errors. It does no queue
// bookkeeping, so it can be tested with fake handlers, no backend needed.
func runJob(ctx context.Context, job *Job, handle Handle) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	if job.Opts.Timeout <= 0 {
		return handle(ctx, job)
	}
	jobCtx, cancel := context.WithTimeout(ctx, job.Opts.Timeout)
	defer cancel()
	return handle(jobCtx, job)
}

// settle is the policy decision point after one execution attempt: it decides
// the job's next state and persists it. On success the job is completed; on
// failure it is either scheduled for a retry with exponential backoff (while
// attempts remain) or marked as finally failed.
func settle(ctx context.Context, rdb *RDB, job *Job, runErr error) {
	if runErr != nil {
		reason := runErr.Error()
		// Retry while attempts remain, with exponential backoff
		if job.Opts.MaxRetries > 0 && job.retried < job.Opts.MaxRetries {
			next := time.Now().Add(backoff(job.retried))
			if err := retry(3, func() error { return rdb.markAsRetry(ctx, job, next) }); err != nil {
				slog.Error("markAsRetry failed after retries",
					"job_id", job.ID, "type", job.Type, "error", err)
			}
			return
		}
		// Retries exhausted: final failure.
		if err := retry(3, func() error { return rdb.markAsFailed(ctx, job, reason) }); err != nil {
			slog.Error("markAsFailed failed after retries",
				"job_id", job.ID, "type", job.Type, "reason", reason, "error", err)
		}
		return
	}
	if err := retry(3, func() error { return rdb.markAsCompleted(ctx, job) }); err != nil {
		slog.Error("markAsCompleted failed after retries",
			"job_id", job.ID, "type", job.Type, "error", err)
	}
}

const maxBackoff = 30 * time.Second

// backoff returns the wait before retry number retried (0-based), doubling
// each time: 1s, 2s, 4s, ...
func backoff(retried int) time.Duration {
	d := time.Duration(1<<retried) * time.Second
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func workerLoop(ctx context.Context, wg *sync.WaitGroup, rdb *RDB, handlemap H) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, err := rdb.dequeue(ctx, defaultQueueName) // TODO: now use only defaultQueueName
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}
		handle, ok := handlemap[job.Type]
		if !ok {
			settle(ctx, rdb, job, errNoHandler)
			continue
		}
		settle(ctx, rdb, job, runJob(ctx, job, handle))
	}
}

// forwardLoop launches a loop that forwards every second.
func forwardLoop(ctx context.Context, wg *sync.WaitGroup, rdb *RDB) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
		for {
			var n int64
			err := retry(3, func() error {
				var ferr error
				n, ferr = rdb.forward(ctx, defaultQueueName)
				return ferr
			})
			if err != nil {
				slog.Error("forward failed after retries", "error", err)
				break
			}
			if n == forwardBatchSize {
				continue
			}
			break
		}
	}
}

// recoveryLoop periodically reclaims jobs whose running lease has expired.
// It runs every second, so recovery latency is roughly leaseDuration + 1s.
func recoveryLoop(ctx context.Context, wg *sync.WaitGroup, rdb *RDB) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
		for {
			var n int64
			err := retry(3, func() error {
				var ferr error
				n, ferr = rdb.recover(ctx, defaultQueueName)
				return ferr
			})
			if err != nil {
				slog.Error("recover failed after retries", "error", err)
				break
			}
			if n == recoverBatchSize {
				continue
			}
			break
		}
	}
}

func retry(attempts int, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return err
}
