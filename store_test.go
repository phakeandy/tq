package taskqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	taskqueue "github.com/phakeandy/task-queue"
)

func setupStorer(t *testing.T) *taskqueue.Storer {
	t.Helper()
	if err := testRdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}
	return taskqueue.NewStorer(testRdb)
}

// newTestTask is a test helper that returns a new testing Task.
func newTestTask(typename string, payload []byte) *taskqueue.Task {
	task, err := taskqueue.NewTask(typename, payload)
	if err != nil {
		panic("newTestTask: " + err.Error())
	}
	return task
}

func TestStorer_Enqueue(t *testing.T) {
	ctx := context.Background()

	t.Run("enqueue a single task", func(t *testing.T) {
		s := setupStorer(t)
		task := newTestTask("email", []byte(`{"to":"a@b.com"}`))
		t.Log(task)

		err := s.Enqueue(ctx, task)
		if err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}

		// Check the queue length in Redis.
		length, err := testRdb.LLen(ctx, "taskqueue:queue").Result()
		if err != nil {
			t.Fatalf("LLen error = %v", err)
		}
		if length != 1 {
			t.Errorf("queue length = %d, want 1", length)
		}

		// Check the hash for this task exists in Redis.
		exists, err := testRdb.Exists(ctx, "taskqueue:task"+task.ID().String()).Result()
		if err != nil {
			t.Fatalf("Exists error = %v", err)
		}
		if exists != 1 {
			t.Errorf("task key not found in Redis")
		}
	})
}

func TestStorer_Dequeue(t *testing.T) {
	ctx := context.Background()

	t.Run("dequeue from empty queue returns redis.Nil", func(t *testing.T) {
		s := setupStorer(t)
		var task taskqueue.Task
		id, err := s.Dequeue(ctx, &task)
		if !errors.Is(err, redis.Nil) {
			t.Errorf("expected redis.Nil, got %v", err)
		}
		if id != uuid.Nil {
			t.Errorf("expected uuid.Nil, got %v", id)
		}
	})

	t.Run("enqueue then dequeue round-trip", func(t *testing.T) {
		s := setupStorer(t)
		original := newTestTask("sms", []byte(`{"phone":"123"}`))

		// Enqueue
		if err := s.Enqueue(ctx, original); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}

		// Dequeue
		var dequeued taskqueue.Task
		id, err := s.Dequeue(ctx, &dequeued)
		if err != nil {
			t.Fatalf("Dequeue() error = %v", err)
		}
		if id != original.ID() {
			t.Errorf("dequeued id = %v, want %v", id, original.ID())
		}
		if dequeued.ID() != original.ID() {
			t.Errorf("task.id = %v, want %v", dequeued.ID(), original.ID())
		}

		// The queue should be empty now.
		length, _ := testRdb.LLen(ctx, "taskqueue:queue").Result()
		if length != 0 {
			t.Errorf("queue length after dequeue = %d, want 0", length)
		}

		// Dequeue should have set the status to completed.
		if dequeued.Status() != taskqueue.StatusCompleted {
			t.Errorf("status = %v, want %v", dequeued.Status(), taskqueue.StatusCompleted)
		}

		// The status in Redis should also be updated.
		redisStatus, _ := testRdb.HGet(ctx, "taskqueue:task"+original.ID().String(), "status").Int()
		if taskqueue.TaskStatus(redisStatus) != taskqueue.StatusCompleted {
			t.Errorf("Redis status = %v, want %v", taskqueue.TaskStatus(redisStatus), taskqueue.StatusCompleted)
		}

		// The task spec should survive the round trip intact.
		if dequeued.Typename != "sms" {
			t.Errorf("typename = %q, want %q", dequeued.Typename, "sms")
		}
		if string(dequeued.Payload) != `{"phone":"123"}` {
			t.Errorf("payload = %q, want %q", string(dequeued.Payload), `{"phone":"123"}`)
		}

		// CreatedAt should be in a reasonable range.
		now := time.Now()
		if dequeued.CreatedAt().After(now) {
			t.Errorf("createdAt %v is in the future", dequeued.CreatedAt())
		}
		if now.Sub(dequeued.CreatedAt()) > 10*time.Second {
			t.Errorf("createdAt %v is too old", dequeued.CreatedAt())
		}
	})

	t.Run("FIFO order: first in, first out", func(t *testing.T) {
		s := setupStorer(t)
		tasks := []*taskqueue.Task{
			newTestTask("type-a", []byte("a")),
			newTestTask("type-b", []byte("b")),
			newTestTask("type-c", []byte("c")),
		}
		// Enqueue uses LPush and Dequeue uses RPop, so together they give FIFO order.
		for _, task := range tasks {
			if err := s.Enqueue(ctx, task); err != nil {
				t.Errorf("failed to euqueue task: %v", err)
			}
		}

		var dequeued []uuid.UUID
		for range tasks {
			var t2 taskqueue.Task
			id, err := s.Dequeue(ctx, &t2)
			if err != nil {
				t.Fatalf("Dequeue() error = %v", err)
			}
			dequeued = append(dequeued, id)
		}

		// FIFO: the first task enqueued should be the first one dequeued.
		for i, task := range tasks {
			if dequeued[i] != task.ID() {
				t.Errorf("position %d: got %s, want %s", i, dequeued[i], task.ID())
			}
		}
	})
}
