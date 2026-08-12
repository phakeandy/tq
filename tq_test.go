package tq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/phakeandy/tq"
	"github.com/phakeandy/tq/internal/testutil"
)

func TestRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client := testutil.SetupRedis(t)
	r := tq.NewRDB(client)

	sendEmail := func(ctx context.Context, job *tq.Job) error {
		var p struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return err
		}
		fmt.Printf("Send a email from: %v, to: %v\n", p.From, p.To)
		return nil
	}

	h := tq.H{
		"email:send": sendEmail,
	}

	err := tq.Run(ctx, r, h, 3)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

// startRun launches Run in a goroutine under a bounded ctx. The Cleanup
// cancels it and asserts that all workers stop cleanly.
func startRun(t *testing.T, r *RDB, handlemap H, concurrency int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- Run(ctx, r, handlemap, concurrency) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Run did not stop after cancel")
		}
	})
}

// waitTerminal polls the job hash until its status leaves pending/running, or
// fails the test when the deadline passes.
func waitTerminal(t *testing.T, client redis.UniversalClient, key string, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		fields := testutil.GetHash(t, client, key)
		switch fields[fieldStatus] {
		case StatusPending.String(), StatusRunning.String():
			time.Sleep(10 * time.Millisecond)
		default:
			return fields
		}
	}
	last := testutil.GetHash(t, client, key)
	t.Fatalf("job %s did not reach a terminal status within %v (last status %q)",
		key, timeout, last[fieldStatus])
	return nil
}

// enqueueTask enqueues a task and returns the Redis key of the resulting job.
func enqueueTask(t *testing.T, r *RDB, client redis.UniversalClient, task *Task) string {
	t.Helper()
	if err := r.enqueue(context.Background(), defaultQueueName, task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
	if len(pending) != 1 {
		t.Fatalf("pending list has %d entries, want 1", len(pending))
	}
	return fmt.Sprintf(keyJob, defaultQueueName, pending[0])
}

// TestRunTimeoutExpires verifies F8: a job whose Timeout elapses mid-execution
// is aborted via ctx cancellation and ends up failed with ctx.Err().
func TestRunTimeoutExpires(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r, client,
		NewTask("slow:task", []byte(`{}`), WithTimeout(50*time.Millisecond)))

	slow := func(ctx context.Context, j *Job) error {
		select {
		case <-time.After(500 * time.Millisecond):
			return nil // 不应到达：50ms 超时应先触发
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	startRun(t, r, H{"slow:task": slow}, 1)

	fields := waitTerminal(t, client, jobKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if errStr := fields[fieldError]; !strings.Contains(errStr, "context deadline exceeded") {
		t.Errorf("error = %q, want it to contain %q", errStr, "context deadline exceeded")
	}
}

// TestRunTimeoutZeroMeansNoTimeout verifies the WithTimeout(0) contract: zero
// disables the timeout entirely, so a slow-but-finite handler must complete.
func TestRunTimeoutZeroMeansNoTimeout(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r, client,
		NewTask("slow:task", []byte(`{}`), WithTimeout(0)))

	slow := func(ctx context.Context, j *Job) error {
		select {
		case <-time.After(300 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	startRun(t, r, H{"slow:task": slow}, 1)

	fields := waitTerminal(t, client, jobKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("status = %q, want %q (error: %v)", got, StatusCompleted.String(), fields[fieldError])
	}
}

// TestRunDefaultTimeoutReachesWorker verifies the default-value chain: a task
// created without WithTimeout must carry the 30s default all the way to the
// worker, i.e. options are resolved before serialization (enqueue side).
func TestRunDefaultTimeoutReachesWorker(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r, client, NewTask("echo:task", []byte(`{}`)))

	gotTimeout := make(chan time.Duration, 1)
	handler := func(ctx context.Context, j *Job) error {
		gotTimeout <- j.Opts.Timeout
		return nil
	}
	startRun(t, r, H{"echo:task": handler}, 1)

	fields := waitTerminal(t, client, jobKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("status = %q, want %q", got, StatusCompleted.String())
	}
	select {
	case d := <-gotTimeout:
		if d != 30*time.Second {
			t.Errorf("worker received Timeout = %v, want 30s (default)", d)
		}
	case <-time.After(time.Second):
		t.Fatal("handler was never invoked")
	}
}

// TestRunHandlerError verifies that a handler returning a plain error sends
// the job to the failed state with the error message recorded.
func TestRunHandlerError(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r, client, NewTask("fail:task", []byte(`{}`)))

	fail := func(ctx context.Context, j *Job) error {
		return errors.New("boom")
	}
	startRun(t, r, H{"fail:task": fail}, 1)

	fields := waitTerminal(t, client, jobKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if got := fields[fieldError]; got != "boom" {
		t.Errorf("error = %q, want %q", got, "boom")
	}
}

// TestRunNoHandler verifies that a job whose type has no registered handler
// is marked failed with a descriptive reason.
func TestRunNoHandler(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r, client, NewTask("nobody:home", []byte(`{}`)))

	startRun(t, r, H{"some:other": func(ctx context.Context, j *Job) error { return nil }}, 1)

	fields := waitTerminal(t, client, jobKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if got := fields[fieldError]; got != "no handler for task type" {
		t.Errorf("error = %q, want %q", got, "no handler for task type")
	}
}

// TestRunHandlerPanic verifies F9: a panicking handler fails the job without
// killing the worker — a second job submitted afterwards must still run.
func TestRunHandlerPanic(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	boom := func(ctx context.Context, j *Job) error {
		panic("kaboom")
	}
	ok := func(ctx context.Context, j *Job) error { return nil }
	startRun(t, r, H{"panic:task": boom, "ok:task": ok}, 1)

	// First job: handler panics, must end up failed with the panic message.
	panicKey := enqueueTask(t, r, client, NewTask("panic:task", []byte(`{}`)))
	fields := waitTerminal(t, client, panicKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if got := fields[fieldError]; !strings.Contains(got, "panic: kaboom") {
		t.Errorf("error = %q, want it to contain %q", got, "panic: kaboom")
	}

	// Second job: same worker (concurrency=1) must still be alive and complete it.
	okKey := enqueueTask(t, r, client, NewTask("ok:task", []byte(`{}`)))
	fields = waitTerminal(t, client, okKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("worker died after panic: second job status = %q, want %q",
			got, StatusCompleted.String())
	}
}

// ──────────────────────────── runJob (pure, no Redis) ────────────────────────────

// TestRunJobTimeout verifies that a job with a positive Timeout gets a
// deadline-capped context: the handler blocks until cancellation and the
// deadline error propagates.
func TestRunJobTimeout(t *testing.T) {
	handle := func(ctx context.Context, job *Job) error {
		<-ctx.Done() // block until the timeout cancels the ctx
		return ctx.Err()
	}
	job := &Job{JobBody: JobBody{Opts: options{Timeout: 10 * time.Millisecond}}}

	err := runJob(context.Background(), job, handle)
	if err != context.DeadlineExceeded {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
}

// TestRunJobPanic verifies that a panicking handler is converted to an error.
func TestRunJobPanic(t *testing.T) {
	handle := func(ctx context.Context, job *Job) error { panic("boom") }

	err := runJob(context.Background(), &Job{}, handle)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("got %v, want panic converted to error", err)
	}
}

// TestRunJobSuccess verifies the plain path: nil error passes through,
// and a zero Timeout means no deadline is applied.
func TestRunJobSuccess(t *testing.T) {
	handle := func(ctx context.Context, job *Job) error {
		if _, ok := ctx.Deadline(); ok {
			return errors.New("expected no deadline for Timeout=0")
		}
		return nil
	}

	if err := runJob(context.Background(), &Job{}, handle); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}

