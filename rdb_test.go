package tq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/phakeandy/tq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

// samplePayload is a small JSON blob used across tests.
var samplePayload = []byte(`{"from":"a@example.com","to":"b@example.com"}`)

// ──────────────────────────── seed helpers ────────────────────────────
//
// These write a job directly into Redis, mimicking what enqueue/dequeue do,
// so individual operations can be tested in isolation.  They live here (not
// in testutil) because they need unexported tq symbols.

func seedPendingJob(tb testing.TB, client redis.UniversalClient, job *Job) {
	tb.Helper()
	ctx := context.Background()
	body, err := json.Marshal(job.JobBody)
	if err != nil {
		tb.Fatal(err)
	}
	client.HSet(ctx, jobKey(job),
		fieldBody, string(body),
		fieldStatus, StatusPending.String(),
		fieldPendingSince, time.Now().Unix(),
	)
	client.LPush(ctx, pendingKey(job.qname), job.ID.String())
}

func seedRunningJob(tb testing.TB, client redis.UniversalClient, job *Job) {
	tb.Helper()
	ctx := context.Background()
	body, err := json.Marshal(job.JobBody)
	if err != nil {
		tb.Fatal(err)
	}
	client.HSet(ctx, jobKey(job),
		fieldBody, string(body),
		fieldStatus, StatusRunning.String(),
	)
	client.ZAdd(ctx, runningKey(job.qname), redis.Z{
		Member: job.ID.String(),
		Score:  float64(time.Now().Add(defaultLeaseDuration).Unix()),
	})
}

// seedDelayedJob writes a job into one of the delayed zsets (scheduled/retry)
// with the given status and due time, mimicking what enqueue/retry do.
func seedDelayedJob(tb testing.TB, client redis.UniversalClient, job *Job, zsetKey string, status Status, due time.Time) {
	tb.Helper()
	ctx := context.Background()
	body, err := json.Marshal(job.JobBody)
	if err != nil {
		tb.Fatal(err)
	}
	client.HSet(ctx, jobKey(job),
		fieldBody, string(body),
		fieldStatus, status.String(),
	)
	client.ZAdd(ctx, zsetKey, redis.Z{
		Member: job.ID.String(),
		Score:  float64(due.Unix()),
	})
}

// ──────────────────────────── enqueue ────────────────────────────

func TestEnqueue(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	tests := []struct {
		desc string
		task *Task
	}{
		{desc: "simple task", task: NewTask("email:send", samplePayload)},
		{desc: "another task type", task: NewTask("report:generate", []byte(`{}`))},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			testutil.FlushDB(t, client)

			id, err := r.enqueue(context.Background(), defaultQueueName, tc.task)
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			job := &Job{JobBody: JobBody{ID: id.ID}, qname: defaultQueueName}
			fields := testutil.GetHash(t, client, jobKey(job))

			if got := fields[fieldStatus]; got != StatusPending.String() {
				t.Errorf("status = %q, want %q", got, StatusPending.String())
			}

			var gotBody JobBody
			if err := json.Unmarshal([]byte(fields[fieldBody]), &gotBody); err != nil {
				t.Fatalf("unmarshal body: %v", err)
			}
			if gotBody.Type != tc.task.typ {
				t.Errorf("Type = %q, want %q", gotBody.Type, tc.task.typ)
			}
			if string(gotBody.Payload) != string(tc.task.payload) {
				t.Errorf("Payload = %s, want %s", gotBody.Payload, tc.task.payload)
			}
		})
	}
}

func TestEnqueueScheduled(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	const delay = 30 * time.Second
	task := NewTask("email:send", samplePayload, WithDelay(delay))

	testutil.FlushDB(t, client)

	before := time.Now().Unix()
	res, err := r.enqueue(context.Background(), defaultQueueName, task)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	id := res.ID
	after := time.Now().Unix()

	// A delayed job must NOT land in the pending list.
	pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
	if len(pending) != 0 {
		t.Fatalf("pending list has %d entries, want 0", len(pending))
	}

	// It must land in the scheduled zset, with score == process_at.
	scheduled := testutil.GetZSet(t, client, scheduledKey(defaultQueueName))
	if len(scheduled) != 1 {
		t.Fatalf("scheduled zset has %d entries, want 1", len(scheduled))
	}
	score := int64(scheduled[0].Score)
	secs := int64(delay / time.Second)
	if min, max := before+secs, after+secs; score < min || score > max {
		t.Errorf("scheduled score = %d, want in [%d, %d]", score, min, max)
	}

	member, ok := scheduled[0].Member.(string)
	if !ok {
		t.Fatalf("scheduled member is not a string: %v", scheduled[0].Member)
	}
	if member != id.String() {
		t.Errorf("scheduled member = %q, want %q", member, id.String())
	}
	job := &Job{JobBody: JobBody{ID: id}, qname: defaultQueueName}
	fields := testutil.GetHash(t, client, jobKey(job))

	if got := fields[fieldStatus]; got != StatusScheduled.String() {
		t.Errorf("status = %q, want %q", got, StatusScheduled.String())
	}

	var gotBody JobBody
	if err := json.Unmarshal([]byte(fields[fieldBody]), &gotBody); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if gotBody.Type != task.typ {
		t.Errorf("Type = %q, want %q", gotBody.Type, task.typ)
	}
	if string(gotBody.Payload) != string(task.payload) {
		t.Errorf("Payload = %s, want %s", gotBody.Payload, task.payload)
	}
	// The delay survives the round-trip through the serialized options.
	if gotBody.Opts.Delay != delay {
		t.Errorf("Delay = %v, want %v", gotBody.Opts.Delay, delay)
	}
}

// ──────────────────────────── dequeue ────────────────────────────

func TestDequeue(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	t.Run("moves job from pending to running", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
		seedPendingJob(t, client, job)

		got, err := r.dequeue(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if got.ID != job.ID {
			t.Errorf("ID = %v, want %v", got.ID, job.ID)
		}
		if got.Type != job.Type {
			t.Errorf("Type = %q, want %q", got.Type, job.Type)
		}

		pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
		if len(pending) != 0 {
			t.Errorf("pending list has %d entries after dequeue, want 0", len(pending))
		}

		running := testutil.GetZSet(t, client, runningKey(defaultQueueName))
		if len(running) != 1 {
			t.Fatalf("running zset has %d entries, want 1", len(running))
		}
		if running[0].Member != job.ID.String() {
			t.Errorf("running member = %v, want %v", running[0].Member, job.ID)
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusRunning.String() {
			t.Errorf("status = %q, want %q", got, StatusRunning.String())
		}
	})

	t.Run("empty queue returns error", func(t *testing.T) {
		testutil.FlushDB(t, client)

		_, err := r.dequeue(context.Background(), defaultQueueName)
		if err == nil {
			t.Fatal("expected error on empty queue, got nil")
		}
	})
}

// ──────────────────────────── forward ────────────────────────────

func TestForward(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	newJob := func() *Job {
		return &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
	}

	t.Run("moves a due scheduled job to pending", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		seedDelayedJob(t, client, job, scheduledKey(defaultQueueName),
			StatusScheduled, time.Now().Add(-time.Second))

		moved, err := r.forward(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("forward: %v", err)
		}
		if moved != 1 {
			t.Errorf("forward moved %d jobs, want 1", moved)
		}

		if got := testutil.GetZSet(t, client, scheduledKey(defaultQueueName)); len(got) != 0 {
			t.Errorf("scheduled zset has %d entries, want 0", len(got))
		}

		pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
		if len(pending) != 1 || pending[0] != job.ID.String() {
			t.Errorf("pending list = %v, want [%s]", pending, job.ID)
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusPending.String() {
			t.Errorf("status = %q, want %q", got, StatusPending.String())
		}
		if _, ok := fields[fieldPendingSince]; !ok {
			t.Errorf("pending_since not set after forward")
		}
	})

	t.Run("leaves a future scheduled job untouched", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		seedDelayedJob(t, client, job, scheduledKey(defaultQueueName),
			StatusScheduled, time.Now().Add(30*time.Second))

		moved, err := r.forward(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("forward: %v", err)
		}
		if moved != 0 {
			t.Errorf("forward moved %d jobs, want 0", moved)
		}

		if got := testutil.GetZSet(t, client, scheduledKey(defaultQueueName)); len(got) != 1 {
			t.Errorf("scheduled zset has %d entries, want 1", len(got))
		}

		if got := testutil.GetList(t, client, pendingKey(defaultQueueName)); len(got) != 0 {
			t.Errorf("pending list has %d entries, want 0", len(got))
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusScheduled.String() {
			t.Errorf("status = %q, want %q", got, StatusScheduled.String())
		}
	})

	t.Run("moves a due retry job to pending", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		seedDelayedJob(t, client, job, retryKey(defaultQueueName),
			StatusRetry, time.Now().Add(-time.Second))

		moved, err := r.forward(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("forward: %v", err)
		}
		if moved != 1 {
			t.Errorf("forward moved %d jobs, want 1", moved)
		}

		if got := testutil.GetZSet(t, client, retryKey(defaultQueueName)); len(got) != 0 {
			t.Errorf("retry zset has %d entries, want 0", len(got))
		}

		pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
		if len(pending) != 1 || pending[0] != job.ID.String() {
			t.Errorf("pending list = %v, want [%s]", pending, job.ID)
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusPending.String() {
			t.Errorf("status = %q, want %q", got, StatusPending.String())
		}
	})

	t.Run("moves scheduled and retry in one call", func(t *testing.T) {
		testutil.FlushDB(t, client)

		sched := newJob()
		retryJob := newJob()
		seedDelayedJob(t, client, sched, scheduledKey(defaultQueueName),
			StatusScheduled, time.Now().Add(-time.Second))
		seedDelayedJob(t, client, retryJob, retryKey(defaultQueueName),
			StatusRetry, time.Now().Add(-time.Second))

		moved, err := r.forward(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("forward: %v", err)
		}
		if moved != 2 {
			t.Errorf("forward moved %d jobs, want 2", moved)
		}

		pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
		if len(pending) != 2 {
			t.Fatalf("pending list has %d entries, want 2", len(pending))
		}
		seen := map[string]bool{pending[0]: true, pending[1]: true}
		if !seen[sched.ID.String()] || !seen[retryJob.ID.String()] {
			t.Errorf("pending list = %v, want both job IDs", pending)
		}
	})

	t.Run("forwarding twice does not duplicate", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		seedDelayedJob(t, client, job, scheduledKey(defaultQueueName),
			StatusScheduled, time.Now().Add(-time.Second))

		moved, err := r.forward(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("first forward: %v", err)
		}
		if moved != 1 {
			t.Fatalf("first forward moved %d jobs, want 1", moved)
		}

		moved, err = r.forward(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("second forward: %v", err)
		}
		if moved != 0 {
			t.Fatalf("second forward moved %d jobs, want 0", moved)
		}

		if got := testutil.GetList(t, client, pendingKey(defaultQueueName)); len(got) != 1 {
			t.Errorf("pending list has %d entries, want 1", len(got))
		}
	})

	t.Run("job due exactly now is forwarded", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		seedDelayedJob(t, client, job, scheduledKey(defaultQueueName),
			StatusScheduled, time.Now())

		moved, err := r.forward(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("forward: %v", err)
		}
		if moved != 1 {
			t.Errorf("forward moved %d jobs, want 1 (score == now must be included)", moved)
		}
	})
}

// ──────────────────────────── markAsCompleted ────────────────────────────

func TestMarkAsCompleted(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	t.Run("moves from running to completed", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
		seedRunningJob(t, client, job)

		if err := r.markAsCompleted(context.Background(), job, []byte(`{"ok":true}`)); err != nil {
			t.Fatalf("markAsCompleted: %v", err)
		}

		running := testutil.GetZSet(t, client, runningKey(defaultQueueName))
		if len(running) != 0 {
			t.Errorf("running zset has %d entries, want 0", len(running))
		}

		completed := testutil.GetZSet(t, client, completedKey(defaultQueueName))
		if len(completed) != 1 {
			t.Fatalf("completed zset has %d entries, want 1", len(completed))
		}
		if completed[0].Member != job.ID.String() {
			t.Errorf("completed member = %v, want %v", completed[0].Member, job.ID)
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusCompleted.String() {
			t.Errorf("status = %q, want %q", got, StatusCompleted.String())
		}
		if got := fields[fieldResult]; got != `{"ok":true}` {
			t.Errorf("result = %q, want %q (handler result must be stored for F5)", got, `{"ok":true}`)
		}
	})

	t.Run("job not in running returns error", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
		err := r.markAsCompleted(context.Background(), job, nil)
		if err == nil {
			t.Fatal("expected error when job is not in running, got nil")
		}
	})
}

// ──────────────────────────── markAsFailed ────────────────────────────

func TestMarkAsFailed(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	t.Run("moves from running to failed with reason", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
		seedRunningJob(t, client, job)

		const reason = "smtp timeout"
		if err := r.markAsFailed(context.Background(), job, reason); err != nil {
			t.Fatalf("markAsFailed: %v", err)
		}

		running := testutil.GetZSet(t, client, runningKey(defaultQueueName))
		if len(running) != 0 {
			t.Errorf("running zset has %d entries, want 0", len(running))
		}

		failed := testutil.GetZSet(t, client, failedKey(defaultQueueName))
		if len(failed) != 1 {
			t.Fatalf("failed zset has %d entries, want 1", len(failed))
		}
		if failed[0].Member != job.ID.String() {
			t.Errorf("failed member = %v, want %v", failed[0].Member, job.ID)
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusFailed.String() {
			t.Errorf("status = %q, want %q", got, StatusFailed.String())
		}
		if got := fields[fieldError]; got != reason {
			t.Errorf("error = %q, want %q", got, reason)
		}
	})

	t.Run("job not in running returns error", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
		err := r.markAsFailed(context.Background(), job, "reason")
		if err == nil {
			t.Fatal("expected error when job is not in running, got nil")
		}
	})
}

// ──────────────────────────── markAsRetry ────────────────────────────

func TestMarkAsRetry(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	newJob := func() *Job {
		return &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
	}

	t.Run("moves from running to retry and bumps the counter", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		seedRunningJob(t, client, job)

		next := time.Now().Add(2 * time.Second)
		if err := r.markAsRetry(context.Background(), job, next); err != nil {
			t.Fatalf("markAsRetry: %v", err)
		}

		running := testutil.GetZSet(t, client, runningKey(defaultQueueName))
		if len(running) != 0 {
			t.Errorf("running zset has %d entries, want 0", len(running))
		}

		retrySet := testutil.GetZSet(t, client, retryKey(defaultQueueName))
		if len(retrySet) != 1 {
			t.Fatalf("retry zset has %d entries, want 1", len(retrySet))
		}
		if retrySet[0].Member != job.ID.String() {
			t.Errorf("retry member = %v, want %v", retrySet[0].Member, job.ID)
		}
		if got := int64(retrySet[0].Score); got != next.Unix() {
			t.Errorf("retry score = %d, want %d (nextRetryAt)", got, next.Unix())
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusRetry.String() {
			t.Errorf("status = %q, want %q", got, StatusRetry.String())
		}
		if got := fields[fieldRetried]; got != "1" {
			t.Errorf("retried = %q, want %q", got, "1")
		}
	})

	t.Run("counter survives a full retry cycle", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		seedRunningJob(t, client, job)
		ctx := context.Background()

		if err := r.markAsRetry(ctx, job, time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("first markAsRetry: %v", err)
		}
		// The retry time has passed: forward moves the job back to pending,
		// dequeue to running — as the real loop does. The counter rides along.
		if moved, err := r.forward(ctx, defaultQueueName); err != nil || moved != 1 {
			t.Fatalf("forward: moved=%d err=%v", moved, err)
		}
		job2, err := r.dequeue(ctx, defaultQueueName)
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if job2.retried != 1 {
			t.Errorf("dequeued retried = %d, want 1", job2.retried)
		}

		if err := r.markAsRetry(ctx, job2, time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("second markAsRetry: %v", err)
		}
		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldRetried]; got != "2" {
			t.Errorf("retried = %q, want %q", got, "2")
		}
	})

	t.Run("job not in running returns error", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newJob()
		err := r.markAsRetry(context.Background(), job, time.Now())
		if err == nil {
			t.Fatal("expected error when job is not in running, got nil")
		}
	})
}

// ──────────────────────────── full lifecycle ────────────────────────────

func TestFullLifecycle(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// 1. Enqueue.
	task := NewTask("email:send", samplePayload)
	if _, err := r.enqueue(ctx, defaultQueueName, task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 2. Dequeue.
	job, err := r.dequeue(ctx, defaultQueueName)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if job.Type != task.typ {
		t.Errorf("dequeue().Type = %q, want %q", job.Type, task.typ)
	}

	running := testutil.GetZSet(t, client, runningKey(defaultQueueName))
	if len(running) != 1 {
		t.Fatalf("running zset has %d entries, want 1", len(running))
	}

	// 3. Complete.
	if err := r.markAsCompleted(ctx, job, nil); err != nil {
		t.Fatalf("markAsCompleted: %v", err)
	}

	running = testutil.GetZSet(t, client, runningKey(defaultQueueName))
	if len(running) != 0 {
		t.Errorf("running zset has %d entries after complete, want 0", len(running))
	}
	completed := testutil.GetZSet(t, client, completedKey(defaultQueueName))
	if len(completed) != 1 {
		t.Fatalf("completed zset has %d entries, want 1", len(completed))
	}

	fields := testutil.GetHash(t, client, jobKey(job))
	if got := fields[fieldStatus]; got != StatusCompleted.String() {
		t.Errorf("status = %q, want %q", got, StatusCompleted.String())
	}
}

// ──────────────────────────── idempotency (F5) ────────────────────────────

// TestEnqueueIdempotency covers PRD F5 at the RDB level: the same idempotency
// key must never produce a second job while the first is in flight (reject),
// must resolve to the original outcome once the first is terminal (completed:
// return the stored result; finally failed: return the failure), must be
// race-free under concurrent submission (N1), and must expire after the TTL.
func TestEnqueueIdempotency(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	t.Run("duplicate while pending is rejected", func(t *testing.T) {
		testutil.FlushDB(t, client)

		first, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k1")))
		if err != nil {
			t.Fatalf("first enqueue: %v", err)
		}

		dup, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k1")))
		if !errors.Is(err, ErrDuplicateInFlight) {
			t.Fatalf("second enqueue error = %v, want ErrDuplicateInFlight", err)
		}
		var dupErr *DuplicateError
		if !errors.As(err, &dupErr) {
			t.Fatalf("error type = %T, want *DuplicateError", err)
		}
		if dupErr.ID != first.ID {
			t.Errorf("DuplicateError.ID = %v, want %v (the existing job)", dupErr.ID, first.ID)
		}
		if dup != nil {
			t.Errorf("duplicate result = %+v, want nil when rejected", dup)
		}

		// Exactly one job exists.
		pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
		if len(pending) != 1 {
			t.Errorf("pending list has %d entries, want 1", len(pending))
		}
		if pending[0] != first.ID.String() {
			t.Errorf("pending[0] = %q, want %q", pending[0], first.ID)
		}
	})

	t.Run("duplicate while scheduled is rejected", func(t *testing.T) {
		testutil.FlushDB(t, client)

		if _, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k2"), WithDelay(time.Hour))); err != nil {
			t.Fatalf("first enqueue: %v", err)
		}
		if _, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k2"), WithDelay(time.Hour))); !errors.Is(err, ErrDuplicateInFlight) {
			t.Fatalf("second enqueue error = %v, want ErrDuplicateInFlight (scheduled is in flight)", err)
		}
		if got := testutil.GetZSet(t, client, scheduledKey(defaultQueueName)); len(got) != 1 {
			t.Errorf("scheduled zset has %d entries, want 1", len(got))
		}
	})

	t.Run("duplicate of a completed job returns the stored result", func(t *testing.T) {
		testutil.FlushDB(t, client)

		first, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k3")))
		if err != nil {
			t.Fatalf("first enqueue: %v", err)
		}

		// Drive the job to completion with a stored result.
		job, err := r.dequeue(ctx, defaultQueueName)
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if err := r.markAsCompleted(ctx, job, []byte(`{"ok":true}`)); err != nil {
			t.Fatalf("markAsCompleted: %v", err)
		}

		dup, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k3")))
		if err != nil {
			t.Fatalf("duplicate enqueue: %v", err)
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
		if dup.ErrMsg != "" {
			t.Errorf("dup.ErrMsg = %q, want empty for a completed job", dup.ErrMsg)
		}

		// No second job was created.
		if got := testutil.GetList(t, client, pendingKey(defaultQueueName)); len(got) != 0 {
			t.Errorf("pending list has %d entries, want 0", len(got))
		}
		if got := testutil.GetZSet(t, client, completedKey(defaultQueueName)); len(got) != 1 {
			t.Errorf("completed zset has %d entries, want 1", len(got))
		}
	})

	t.Run("duplicate of a finally failed job returns the failure", func(t *testing.T) {
		testutil.FlushDB(t, client)

		first, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k4")))
		if err != nil {
			t.Fatalf("first enqueue: %v", err)
		}

		job, err := r.dequeue(ctx, defaultQueueName)
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if err := r.markAsFailed(ctx, job, "boom"); err != nil {
			t.Fatalf("markAsFailed: %v", err)
		}

		dup, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k4")))
		if err != nil {
			t.Fatalf("duplicate enqueue: %v", err)
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
		if len(dup.Result) != 0 {
			t.Errorf("dup.Result = %q, want empty for a failed job", dup.Result)
		}
	})

	t.Run("empty key disables dedup", func(t *testing.T) {
		testutil.FlushDB(t, client)

		a, err := r.enqueue(ctx, defaultQueueName, NewTask("email:send", samplePayload))
		if err != nil {
			t.Fatalf("first enqueue: %v", err)
		}
		b, err := r.enqueue(ctx, defaultQueueName, NewTask("email:send", samplePayload))
		if err != nil {
			t.Fatalf("second enqueue: %v", err)
		}
		if a.ID == b.ID {
			t.Error("two keyless tasks must be distinct jobs")
		}
		if got := testutil.GetList(t, client, pendingKey(defaultQueueName)); len(got) != 2 {
			t.Errorf("pending list has %d entries, want 2", len(got))
		}
	})

	// N1: concurrent submissions of the same key must create exactly one job.
	t.Run("concurrent submissions of the same key create exactly one job", func(t *testing.T) {
		testutil.FlushDB(t, client)

		const workers = 16
		start := make(chan struct{})
		type outcome struct {
			res *EnqueueResult
			err error
		}
		results := make(chan outcome, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				res, err := r.enqueue(ctx, defaultQueueName,
					NewTask("email:send", samplePayload, WithIdempotencyKey("k-race")))
				results <- outcome{res: res, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		var created int
		var rejected int
		for res := range results {
			if res.err != nil {
				if !errors.Is(res.err, ErrDuplicateInFlight) {
					t.Fatalf("enqueue: %v", res.err)
				}
				rejected++
				continue
			}
			if res.res.Duplicate {
				t.Error("a non-error enqueue must not be a duplicate")
			}
			created++
		}
		if created != 1 {
			t.Errorf("created %d jobs, want exactly 1", created)
		}
		if rejected != workers-1 {
			t.Errorf("rejected %d submissions, want %d", rejected, workers-1)
		}
		if got := testutil.GetList(t, client, pendingKey(defaultQueueName)); len(got) != 1 {
			t.Errorf("pending list has %d entries, want 1", len(got))
		}
	})

	t.Run("key expires after the TTL", func(t *testing.T) {
		testutil.FlushDB(t, client)
		r.uniqueTTL = time.Second

		first, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k-ttl")))
		if err != nil {
			t.Fatalf("first enqueue: %v", err)
		}

		time.Sleep(1100 * time.Millisecond) // key is gone; the job may still exist

		second, err := r.enqueue(ctx, defaultQueueName,
			NewTask("email:send", samplePayload, WithIdempotencyKey("k-ttl")))
		if err != nil {
			t.Fatalf("enqueue after TTL: %v", err)
		}
		if second.Duplicate {
			t.Error("want a fresh job after the idempotency key expired")
		}
		if second.ID == first.ID {
			t.Error("want a different job after the idempotency key expired")
		}
		if got := testutil.GetList(t, client, pendingKey(defaultQueueName)); len(got) != 2 {
			t.Errorf("pending list has %d entries, want 2 (both jobs)", len(got))
		}
	})
}
