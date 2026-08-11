package tq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestManual_markAsCompleted(t *testing.T) {
	t.Skip("it is just for debuging.")

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	r := NewRDB(client)

	// Fixed id so it is easy to find in redis.
	jobID := uuid.MustParse("c2f1b4e0-0000-4000-8000-000000000001")
	job := &Job{
		ID:      jobID,
		Type:    "email:send",
		Payload: []byte(`{"from":"a@example.com","to":"b@example.com"}`),
		qname:   defaultQueueName,
	}

	running := runningKey(job.qname)
	completed := completedKey(job.qname)
	jobKey := jobKey(job)

	client.Del(ctx, running, completed, jobKey)
	client.RPush(ctx, running, jobID.String())
	client.HSet(ctx, jobKey,
		"type", job.Type,
		"payload", string(job.Payload),
		"status", StatusRunning.String(),
	)

	r.markAsCompleted(ctx, job)

}

func TestManual_markAsFailed(t *testing.T) {
	t.Skip("it is just for debuging.")

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	r := NewRDB(client)

	// Fixed id so it is easy to find in redis.
	jobID := uuid.MustParse("c2f1b4e0-0000-4000-8000-000000000002")
	job := &Job{
		ID:      jobID,
		Type:    "email:send",
		Payload: []byte(`{"from":"a@example.com","to":"b@example.com"}`),
		qname:   defaultQueueName,
	}

	running := runningKey(job.qname)
	failed := failedKey(job.qname)
	jobKey := jobKey(job)

	client.Del(ctx, running, failed, jobKey)
	client.RPush(ctx, running, jobID.String())
	client.HSet(ctx, jobKey,
		"type", job.Type,
		"payload", string(job.Payload),
		"status", StatusRunning.String(),
	)

	r.markAsFailed(ctx, job, "some reason")

}

func TestManual_enqueue(t *testing.T) {
	// t.Skip("it is just for debuging.")

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	r := NewRDB(client)

	pending := pendingKey(defaultQueueName)
	client.Del(ctx, pending)

	task := NewTask("email:send", []byte(`{"from":"a@example.com","to":"b@example.com"}`))
	if err := r.enqueue(ctx, defaultQueueName, task); err != nil {
		t.Fatal(err)
	}

	// Read back what was stored so the effect can be inspected manually.
	// enqueue generates the job id internally, so find it via the pending queue.
	ids, err := client.LRange(ctx, pending, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pending queue: %v", ids)
	for _, id := range ids {
		fields, err := client.HGetAll(ctx, fmt.Sprintf(keyJob, defaultQueueName, id)).Result()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("job %s: %v", id, fields)
	}
}

func TestManual_dequeue(t *testing.T) {
	// t.Skip("it is just for debuging.")

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	r := NewRDB(client)

	// Fixed id so it is easy to find in redis.
	jobID := uuid.MustParse("c2f1b4e0-0000-4000-8000-000000000003")
	job := &Job{
		ID:      jobID,
		Type:    "email:send",
		Payload: []byte(`{"from":"a@example.com","to":"b@example.com"}`),
		qname:   defaultQueueName,
	}
	// Mimic what enqueue writes to the job hash.
	body, err := json.Marshal(JobBody{
		ID:      job.ID,
		Type:    job.Type,
		Payload: job.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}

	pending := pendingKey(job.qname)
	running := runningKey(job.qname)
	jobKey := jobKey(job)

	client.Del(ctx, pending, running, jobKey)
	client.LPush(ctx, pending, jobID.String())
	client.HSet(ctx, jobKey,
		"body", string(body),
		"status", StatusPending.String(),
		"pending_since", time.Now().Unix(),
	)

	got, err := r.dequeue(ctx, job.qname)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("dequeued job: %+v", got)

	// Read back the resulting state so the effect can be inspected manually.
	ids, err := client.LRange(ctx, pending, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pending queue after: %v", ids)

	members, err := client.ZRangeWithScores(ctx, running, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		t.Logf("running: %s lease=%v", m.Member, m.Score)
	}

	fields, err := client.HGetAll(ctx, jobKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("job hash after: %v", fields)
}
