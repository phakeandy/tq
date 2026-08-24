package tq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phakeandy/tq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func TestRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	sendEmail := func(ctx context.Context, job *Job) ([]byte, error) {
		var p struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return nil, err
		}
		fmt.Printf("Send a email from: %v, to: %v\n", p.From, p.To)
		return nil, nil
	}

	h := H{
		"email:send": sendEmail,
	}

	err := Run(ctx, r, h, 3)
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

// waitTerminal polls the job hash until its status leaves the in-flight
// states (pending/running/scheduled/retry), or fails the test when the
// deadline passes. Terminal states are completed and failed (final).
func waitTerminal(t *testing.T, client redis.UniversalClient, key string, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		fields := testutil.GetHash(t, client, key)
		switch fields[fieldStatus] {
		case StatusPending.String(), StatusRunning.String(),
			StatusScheduled.String(), StatusRetry.String():
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
func enqueueTask(t *testing.T, r *RDB, task *Task) string {
	t.Helper()
	res, err := r.enqueue(context.Background(), defaultQueueName, task)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return fmt.Sprintf(keyJob, defaultQueueName, res.ID)
}

// TestRunDefaultTimeoutReachesWorker verifies the default-value chain: a task
// created without WithTimeout must carry the 30s default all the way to the
// worker, i.e. options are resolved before serialization (enqueue side).
func TestRunDefaultTimeoutReachesWorker(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r, NewTask("echo:task", []byte(`{}`)))

	gotTimeout := make(chan time.Duration, 1)
	handler := func(ctx context.Context, j *Job) ([]byte, error) {
		gotTimeout <- j.Opts.Timeout
		return nil, nil
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

	jobKey := enqueueTask(t, r,
		NewTask("fail:task", []byte(`{}`), WithMaxRetries(0)))

	fail := func(ctx context.Context, j *Job) ([]byte, error) {
		return nil, errors.New("boom")
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

	jobKey := enqueueTask(t, r, NewTask("nobody:home", []byte(`{}`), WithMaxRetries(0)))

	startRun(t, r, H{"some:other": func(ctx context.Context, j *Job) ([]byte, error) { return nil, nil }}, 1)

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

	boom := func(ctx context.Context, j *Job) ([]byte, error) {
		panic("kaboom")
	}
	ok := func(ctx context.Context, j *Job) ([]byte, error) { return nil, nil }
	startRun(t, r, H{"panic:task": boom, "ok:task": ok}, 1)

	// First job: handler panics, must end up failed with the panic message.
	panicKey := enqueueTask(t, r,
		NewTask("panic:task", []byte(`{}`), WithMaxRetries(0)))
	fields := waitTerminal(t, client, panicKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if got := fields[fieldError]; !strings.Contains(got, "panic: kaboom") {
		t.Errorf("error = %q, want it to contain %q", got, "panic: kaboom")
	}

	// Second job: same worker (concurrency=1) must still be alive and complete it.
	okKey := enqueueTask(t, r, NewTask("ok:task", []byte(`{}`)))
	fields = waitTerminal(t, client, okKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("worker died after panic: second job status = %q, want %q",
			got, StatusCompleted.String())
	}
}

// ──────────────────────────── retry (F4) ────────────────────────────

// TestBackoff verifies the exponential backoff schedule from PRD F4:
// 1s, 2s, 4s, ... after the 1st, 2nd, 3rd failure, capped at maxBackoff.
func TestBackoff(t *testing.T) {
	cases := []struct {
		retried int
		want    time.Duration
	}{
		{0, time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{10, maxBackoff}, // 2^10s would be 1024s; the cap must kick in
	}
	for _, c := range cases {
		if got := backoff(c.retried); got != c.want {
			t.Errorf("backoff(%d) = %v, want %v", c.retried, got, c.want)
		}
	}
}

// TestRunRetryThenSuccess verifies F4 end-to-end: a job that fails once is
// retried (with the counter bumped) and then completes on the second attempt.
func TestRunRetryThenSuccess(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	var attempts atomic.Int32
	flaky := func(ctx context.Context, j *Job) ([]byte, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("first attempt fails")
		}
		return nil, nil
	}

	jobKey := enqueueTask(t, r,
		NewTask("flaky:task", []byte(`{}`), WithMaxRetries(1)))
	startRun(t, r, H{"flaky:task": flaky}, 1)

	fields := waitTerminal(t, client, jobKey, 5*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("status = %q, want %q (error: %v)", got, StatusCompleted.String(), fields[fieldError])
	}
	if got := fields[fieldRetried]; got != "1" {
		t.Errorf("retried = %q, want %q (one retry happened)", got, "1")
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("handler ran %d times, want 2", got)
	}
}

// TestRunRetryExhaustion verifies that once MaxRetries is reached the job is
// marked as finally failed, with the last error recorded and the retry
// counter at its maximum.
func TestRunRetryExhaustion(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r,
		NewTask("fail:task", []byte(`{}`), WithMaxRetries(1)))

	fail := func(ctx context.Context, j *Job) ([]byte, error) { return nil, errors.New("always fails") }
	startRun(t, r, H{"fail:task": fail}, 1)

	fields := waitTerminal(t, client, jobKey, 8*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if got := fields[fieldError]; got != "always fails" {
		t.Errorf("error = %q, want %q", got, "always fails")
	}
	if got := fields[fieldRetried]; got != "1" {
		t.Errorf("retried = %q, want %q", got, "1")
	}
}

// TestRunRetryZero verifies the WithMaxRetries(0) contract: no retry is
// scheduled and the job fails immediately, so the retried field stays absent.
func TestRunRetryZero(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	jobKey := enqueueTask(t, r,
		NewTask("fail:task", []byte(`{}`), WithMaxRetries(0)))

	fail := func(ctx context.Context, j *Job) ([]byte, error) { return nil, errors.New("boom") }
	startRun(t, r, H{"fail:task": fail}, 1)

	fields := waitTerminal(t, client, jobKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if got := fields[fieldError]; got != "boom" {
		t.Errorf("error = %q, want %q", got, "boom")
	}
	if _, ok := fields[fieldRetried]; ok {
		t.Errorf("retried field = %q, want absent (MaxRetries(0) must not retry)", fields[fieldRetried])
	}
}

// ──────────────────────────── runJob (pure, no Redis) ────────────────────────────

// TestRunJobTimeout verifies that a job with a positive Timeout gets a
// deadline-capped context: the handler blocks until cancellation and the
// deadline error propagates.
func TestRunJobTimeout(t *testing.T) {
	handle := func(ctx context.Context, job *Job) ([]byte, error) {
		<-ctx.Done() // block until the timeout cancels the ctx
		return nil, ctx.Err()
	}
	job := &Job{JobBody: JobBody{Opts: options{Timeout: 10 * time.Millisecond}}}

	result, err := runJob(context.Background(), job, handle)
	if err != context.DeadlineExceeded {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	if result != nil {
		t.Errorf("result = %q, want nil on timeout", result)
	}
}

// TestRunJobPanic verifies that a panicking handler is converted to an error.
func TestRunJobPanic(t *testing.T) {
	handle := func(ctx context.Context, job *Job) ([]byte, error) { panic("boom") }

	_, err := runJob(context.Background(), &Job{}, handle)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("got %v, want panic converted to error", err)
	}
}

// TestRunJobSuccess verifies the plain path: nil error passes through, the
// handler's result is returned, and a zero Timeout means no deadline is applied.
func TestRunJobSuccess(t *testing.T) {
	handle := func(ctx context.Context, job *Job) ([]byte, error) {
		if _, ok := ctx.Deadline(); ok {
			return nil, errors.New("expected no deadline for Timeout=0")
		}
		return []byte("done"), nil
	}

	result, err := runJob(context.Background(), &Job{}, handle)
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if string(result) != "done" {
		t.Errorf("result = %q, want %q", result, "done")
	}
}

// enqueueDelayedTask enqueues a delayed task and returns its job key.
func enqueueDelayedTask(t *testing.T, r *RDB, task *Task) string {
	t.Helper()
	res, err := r.enqueue(context.Background(), defaultQueueName, task)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return fmt.Sprintf(keyJob, defaultQueueName, res.ID)
}

// TestRunDelayedTask verifies F3 end-to-end: a job submitted with WithDelay
// must (1) NOT run before its due time, and (2) run and complete once the
// delay elapses. (2) only happens if something periodically forwards the
// scheduled zset into the pending list — that something is the forwardLoop
// you are writing, so this test is red until Run wires it in.
func TestRunDelayedTask(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	// 2s: large enough that second-granularity scores (Unix()) in the Lua
	// scripts cannot truncate it into the "due now" branch of enqueue.
	const delay = 2 * time.Second
	submittedAt := time.Now()
	jobKey := enqueueDelayedTask(t, r,
		NewTask("slow:task", []byte(`{}`), WithDelay(delay)))

	// The handler records the wall-clock time it actually started.
	executedAt := make(chan time.Time, 1)
	handler := func(ctx context.Context, j *Job) ([]byte, error) {
		executedAt <- time.Now()
		return nil, nil
	}
	startRun(t, r, H{"slow:task": handler}, 1)

	// (1) The job must eventually reach a terminal state. Red today: with no
	// forwardLoop wired into Run, the job stays in the scheduled zset forever.
	fields := waitTerminal(t, client, jobKey, 5*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("status = %q, want %q", got, StatusCompleted.String())
	}

	// (2) It must not have run before its due time (N2). The 1s tolerance
	// covers second-granularity scores: the earliest a job can be forwarded
	// is the second boundary of its due time, i.e. up to ~1s "early".
	select {
	case got := <-executedAt:
		earliest := submittedAt.Add(delay - time.Second)
		if got.Before(earliest) {
			t.Errorf("job ran %v after submit (delay %v) — before earliest allowed %v",
				got.Sub(submittedAt), delay, earliest)
		}
	case <-time.After(time.Second):
		t.Fatal("handler was never invoked")
	}
}

// ──────────────────────────── idempotency (F5) ────────────────────────────

// TestRunIdempotentReject verifies F5 end-to-end: while a task with the key is
// in flight, Enqueue refuses to create a second job (ErrDuplicateInFlight).
func TestRunIdempotentReject(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	block := make(chan struct{})
	handler := func(ctx context.Context, j *Job) ([]byte, error) {
		<-block // hold the job in flight until the duplicate has been attempted
		return []byte("done"), nil
	}
	startRun(t, r, H{"idem:task": handler}, 1)

	first, err := Enqueue(context.Background(), r,
		NewTask("idem:task", []byte(`{}`), WithIdempotencyKey("k1")))
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}

	_, err = Enqueue(context.Background(), r,
		NewTask("idem:task", []byte(`{}`), WithIdempotencyKey("k1")))
	if !errors.Is(err, ErrDuplicateInFlight) {
		t.Fatalf("second Enqueue error = %v, want ErrDuplicateInFlight", err)
	}

	close(block) // let the job finish so Run can wind down cleanly
	jobKey := fmt.Sprintf(keyJob, defaultQueueName, first.ID)
	fields := waitTerminal(t, client, jobKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("status = %q, want %q", got, StatusCompleted.String())
	}
	if got := fields[fieldResult]; got != "done" {
		t.Errorf("result = %q, want %q (handler result must be stored)", got, "done")
	}
}

// TestRunIdempotentDuplicateReturnsResult verifies F5 end-to-end: after a task
// completes, re-submitting the same key returns the original job's ID and
// stored result instead of creating a new job.
func TestRunIdempotentDuplicateReturnsResult(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	handler := func(ctx context.Context, j *Job) ([]byte, error) {
		return []byte(`{"ok":true}`), nil
	}
	startRun(t, r, H{"idem:task": handler}, 1)

	first, err := Enqueue(context.Background(), r,
		NewTask("idem:task", []byte(`{}`), WithIdempotencyKey("k2")))
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	firstKey := fmt.Sprintf(keyJob, defaultQueueName, first.ID)
	fields := waitTerminal(t, client, firstKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Fatalf("status = %q, want %q", got, StatusCompleted.String())
	}

	dup, err := Enqueue(context.Background(), r,
		NewTask("idem:task", []byte(`{}`), WithIdempotencyKey("k2")))
	if err != nil {
		t.Fatalf("duplicate Enqueue: %v", err)
	}
	if !dup.Duplicate {
		t.Error("want Duplicate = true")
	}
	if dup.ID != first.ID {
		t.Errorf("dup.ID = %v, want %v (the original job)", dup.ID, first.ID)
	}
	if string(dup.Result) != `{"ok":true}` {
		t.Errorf("dup.Result = %q, want %q (the original result)", dup.Result, `{"ok":true}`)
	}

	// No second job was created.
	if got := testutil.GetList(t, client, pendingKey(defaultQueueName)); len(got) != 0 {
		t.Errorf("pending list has %d entries after duplicate Enqueue, want 0", len(got))
	}
}

// TestRunIdempotentFailedDuplicate verifies F5 with a finally failed task:
// the duplicate submission resolves to the original failure, never to a new
// execution (PRD F1: same-key tasks execute at most once).
func TestRunIdempotentFailedDuplicate(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	handler := func(ctx context.Context, j *Job) ([]byte, error) {
		return nil, errors.New("boom")
	}
	startRun(t, r, H{"idem:task": handler}, 1)

	first, err := Enqueue(context.Background(), r,
		NewTask("idem:task", []byte(`{}`), WithIdempotencyKey("k3"), WithMaxRetries(0)))
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	firstKey := fmt.Sprintf(keyJob, defaultQueueName, first.ID)
	fields := waitTerminal(t, client, firstKey, 3*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}

	dup, err := Enqueue(context.Background(), r,
		NewTask("idem:task", []byte(`{}`), WithIdempotencyKey("k3"), WithMaxRetries(0)))
	if err != nil {
		t.Fatalf("duplicate Enqueue: %v", err)
	}
	if !dup.Duplicate {
		t.Error("want Duplicate = true")
	}
	if dup.ID != first.ID {
		t.Errorf("dup.ID = %v, want %v (the original job)", dup.ID, first.ID)
	}
	if dup.ErrMsg != "boom" {
		t.Errorf("dup.ErrMsg = %q, want %q (the original failure)", dup.ErrMsg, "boom")
	}

	// The handler ran exactly once: no second execution happened.
	if got := testutil.GetZSet(t, client, failedKey(defaultQueueName)); len(got) != 1 {
		t.Errorf("failed zset has %d entries, want 1", len(got))
	}
}
