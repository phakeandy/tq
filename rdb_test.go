package tq

import (
	"context"
	"encoding/json"
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
		Score:  float64(time.Now().Add(leaseDuration).Unix()),
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

			if err := r.enqueue(context.Background(), defaultQueueName, tc.task); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			// The job ID is generated internally — read it from the pending list.
			pending := testutil.GetList(t, client, pendingKey(defaultQueueName))
			if len(pending) != 1 {
				t.Fatalf("pending list has %d entries, want 1", len(pending))
			}
			jobID, err := uuid.Parse(pending[0])
			if err != nil {
				t.Fatalf("invalid job ID in pending list: %q", pending[0])
			}

			job := &Job{JobBody: JobBody{ID: jobID}, qname: defaultQueueName}
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
	if err := r.enqueue(context.Background(), defaultQueueName, task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
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
	jobID, err := uuid.Parse(member)
	if err != nil {
		t.Fatalf("invalid job ID in scheduled zset: %q", member)
	}
	job := &Job{JobBody: JobBody{ID: jobID}, qname: defaultQueueName}
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

		if err := r.markAsCompleted(context.Background(), job); err != nil {
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
	})

	t.Run("job not in running returns error", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := &Job{
			JobBody: JobBody{ID: uuid.New(), Type: "email:send", Payload: samplePayload},
			qname:   defaultQueueName,
		}
		err := r.markAsCompleted(context.Background(), job)
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

// ──────────────────────────── full lifecycle ────────────────────────────

func TestFullLifecycle(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// 1. Enqueue.
	task := NewTask("email:send", samplePayload)
	if err := r.enqueue(ctx, defaultQueueName, task); err != nil {
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
	if err := r.markAsCompleted(ctx, job); err != nil {
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
