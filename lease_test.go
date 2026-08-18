package tq

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/phakeandy/tq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

// ──────────────────────────── F12 lease recovery ────────────────────────────
//
// These tests are intentionally written before the implementation:
//   - r.recover(...) is the missing RDB primitive that reclaims expired
//     running jobs (analogous to r.forward).
//   - r.leaseDuration is the configurable lease duration (default 30s),
//     replacing the old const leaseDuration (now defaultLeaseDuration).
//
// Until those two symbols exist the package does not compile (expected red).

// seedRunningJobForLease seeds a running job whose lease expires at
// leaseExpireAt, with the retried counter pre-set to retried.
func seedRunningJobForLease(t *testing.T, client redis.UniversalClient, job *Job, leaseExpireAt time.Time, retried int) {
	t.Helper()
	ctx := context.Background()
	body, err := json.Marshal(job.JobBody)
	if err != nil {
		t.Fatal(err)
	}
	client.HSet(ctx, jobKey(job),
		fieldBody, string(body),
		fieldStatus, StatusRunning.String(),
		fieldRetried, strconv.Itoa(retried),
	)
	client.ZAdd(ctx, runningKey(job.qname), redis.Z{
		Member: job.ID.String(),
		Score:  float64(leaseExpireAt.Unix()),
	})
}

func newLeaseJob(maxRetries int) *Job {
	return &Job{
		JobBody: JobBody{
			ID:      uuid.New(),
			Type:    "lease:task",
			Payload: samplePayload,
			Opts:    options{MaxRetries: maxRetries},
		},
		qname: defaultQueueName,
	}
}

// TestDequeueSetsLeaseExpiration locks in the first half of F12: when a job
// enters the running zset, its score is the lease expiration time.
func TestDequeueSetsLeaseExpiration(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	job := newLeaseJob(0)
	seedPendingJob(t, client, job)

	before := time.Now()
	if _, err := r.dequeue(context.Background(), defaultQueueName); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	after := time.Now()

	running := testutil.GetZSet(t, client, runningKey(defaultQueueName))
	if len(running) != 1 {
		t.Fatalf("running zset has %d entries, want 1", len(running))
	}
	if running[0].Member != job.ID.String() {
		t.Errorf("running member = %v, want %v", running[0].Member, job.ID)
	}

	score := int64(running[0].Score)
	min := before.Add(r.leaseDuration).Unix()
	max := after.Add(r.leaseDuration).Unix()
	if score < min || score > max {
		t.Errorf("lease score = %d, want in [%d, %d] (now + leaseDuration)",
			score, min, max)
	}
}

// TestRecover covers the RDB-level primitive that reclaims expired leases.
func TestRecover(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)

	t.Run("moves an expired running job to retry and bumps the counter", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newLeaseJob(1)
		seedRunningJobForLease(t, client, job, time.Now().Add(-time.Second), 0)

		moved, err := r.recover(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		if moved != 1 {
			t.Errorf("recover moved %d jobs, want 1", moved)
		}

		if got := testutil.GetZSet(t, client, runningKey(defaultQueueName)); len(got) != 0 {
			t.Errorf("running zset has %d entries, want 0", len(got))
		}

		retrySet := testutil.GetZSet(t, client, retryKey(defaultQueueName))
		if len(retrySet) != 1 {
			t.Fatalf("retry zset has %d entries, want 1", len(retrySet))
		}
		if retrySet[0].Member != job.ID.String() {
			t.Errorf("retry member = %v, want %v", retrySet[0].Member, job.ID)
		}
		if got := int64(retrySet[0].Score); got <= time.Now().Unix() {
			t.Errorf("retry score = %d, want a future time (backoff applied)", got)
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusRetry.String() {
			t.Errorf("status = %q, want %q", got, StatusRetry.String())
		}
		if got := fields[fieldRetried]; got != "1" {
			t.Errorf("retried = %q, want %q", got, "1")
		}
	})

	t.Run("moves an expired running job to failed when retries are exhausted", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newLeaseJob(1)
		seedRunningJobForLease(t, client, job, time.Now().Add(-time.Second), 1)

		moved, err := r.recover(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		if moved != 1 {
			t.Errorf("recover moved %d jobs, want 1", moved)
		}

		if got := testutil.GetZSet(t, client, runningKey(defaultQueueName)); len(got) != 0 {
			t.Errorf("running zset has %d entries, want 0", len(got))
		}
		if got := testutil.GetZSet(t, client, retryKey(defaultQueueName)); len(got) != 0 {
			t.Errorf("retry zset has %d entries, want 0 (no retries left)", len(got))
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
		if got := fields[fieldError]; got != "lease expired" {
			t.Errorf("error = %q, want %q (recoverer writes its own reason)", got, "lease expired")
		}
		if got := fields[fieldRetried]; got != "1" {
			t.Errorf("retried = %q, want %q (unchanged by failed recovery)", got, "1")
		}
	})

	t.Run("leaves an unexpired running job untouched", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newLeaseJob(1)
		seedRunningJobForLease(t, client, job, time.Now().Add(30*time.Second), 0)

		moved, err := r.recover(context.Background(), defaultQueueName)
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		if moved != 0 {
			t.Errorf("recover moved %d jobs, want 0 (lease not expired)", moved)
		}

		running := testutil.GetZSet(t, client, runningKey(defaultQueueName))
		if len(running) != 1 {
			t.Errorf("running zset has %d entries, want 1 (unexpired job stays)", len(running))
		}

		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldStatus]; got != StatusRunning.String() {
			t.Errorf("status = %q, want %q", got, StatusRunning.String())
		}
	})

	t.Run("concurrent recoverers do not reclaim the same job twice", func(t *testing.T) {
		testutil.FlushDB(t, client)

		job := newLeaseJob(1)
		seedRunningJobForLease(t, client, job, time.Now().Add(-time.Second), 0)

		const workers = 8
		start := make(chan struct{})
		type result struct {
			moved int64
			err   error
		}
		results := make(chan result, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				moved, err := r.recover(context.Background(), defaultQueueName)
				results <- result{moved: moved, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		var total int64
		for res := range results {
			if res.err != nil {
				t.Fatalf("recover: %v", res.err)
			}
			total += res.moved
		}
		if total != 1 {
			t.Errorf("concurrent recoverers moved %d jobs total, want 1 (ZREM == 1 is the anti-duplication guard)", total)
		}

		retrySet := testutil.GetZSet(t, client, retryKey(defaultQueueName))
		if len(retrySet) != 1 {
			t.Errorf("retry zset has %d entries, want 1", len(retrySet))
		}
		fields := testutil.GetHash(t, client, jobKey(job))
		if got := fields[fieldRetried]; got != "1" {
			t.Errorf("retried = %q, want %q (exactly one recovery)", got, "1")
		}
	})
}

// TestRunRecoverExpiredLease is the end-to-end F12 test: a job whose handler
// never finishes within its lease must be recovered, retried, and finally
// failed once retries are exhausted.
func TestRunRecoverExpiredLease(t *testing.T) {
	client := testutil.SetupRedis(t)
	r := NewRDB(client)
	r.leaseDuration = 50 * time.Millisecond

	jobKey := enqueueTask(t, r, client,
		NewTask("stuck:task", []byte(`{}`), WithMaxRetries(1), WithTimeout(0)))

	// The handler blocks until the whole Run is cancelled, which is much
	// longer than the 50ms lease: from the lease's point of view this worker
	// is dead (a real crash would never return either).
	handler := func(ctx context.Context, j *Job) error {
		<-ctx.Done()
		return ctx.Err()
	}
	// concurrency=2: one worker is stuck in the first attempt while another
	// worker must be available to pick up the retry after it is recovered.
	startRun(t, r, H{"stuck:task": handler}, 2)

	fields := waitTerminal(t, client, jobKey, 8*time.Second)
	if got := fields[fieldStatus]; got != StatusFailed.String() {
		t.Fatalf("status = %q, want %q", got, StatusFailed.String())
	}
	if got := fields[fieldError]; got != "lease expired" {
		t.Errorf("error = %q, want %q", got, "lease expired")
	}
	if got := fields[fieldRetried]; got != "1" {
		t.Errorf("retried = %q, want %q (one lease recovery then retries exhausted)", got, "1")
	}
}
