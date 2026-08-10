package tq

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestManual_MarkAsCompleted(t *testing.T) {
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
	jobKey := job.Key()

	client.Del(ctx, running, completed, jobKey)
	client.RPush(ctx, running, jobID.String())
	client.HSet(ctx, jobKey,
		"type", job.Type,
		"payload", string(job.Payload),
		"status", StatusRunning.String(),
	)

	r.markAsCompleted(ctx, job)

}
