package taskqueue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var testRdb redis.UniversalClient

func TestMain(m *testing.M) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6380"
	}

	testRdb = redis.NewClient(&redis.Options{Addr: addr})
	defer testRdb.Close()

	ctx := context.Background()
	if err := testRdb.Ping(ctx).Err(); err != nil {
		panic("cannot connect to Redis at " + addr + ": " + err.Error())
	}

	code := m.Run()
	os.Exit(code)
}

func setupStorer(t *testing.T) *Storer {
	t.Helper()
	if err := testRdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}
	return &Storer{rdb: testRdb}
}

// newTestTask is a test helper that returns a new testing Task.
func newTestTask(typename string, payload []byte) *Task {
	return &Task{
		taskInfo: taskInfo{
			id:     uuid.New(),
			status: StatusWaiting,
		},
		taskSpec: taskSpec{
			Typename: typename,
			Payload:  payload,
		},
	}
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

		// 验证队列长度
		length, err := testRdb.LLen(ctx, prefixKeyQueue).Result()
		if err != nil {
			t.Fatalf("LLen error = %v", err)
		}
		if length != 1 {
			t.Errorf("queue length = %d, want 1", length)
		}

		// 验证 hash 中存在这个 task 的 key
		exists, err := testRdb.Exists(ctx, prefixKeyTask+task.id.String()).Result()
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
		var task Task
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
		var dequeued Task
		id, err := s.Dequeue(ctx, &dequeued)
		if err != nil {
			t.Fatalf("Dequeue() error = %v", err)
		}
		if id != original.id {
			t.Errorf("dequeued id = %v, want %v", id, original.id)
		}
		if dequeued.id != original.id {
			t.Errorf("task.id = %v, want %v", dequeued.id, original.id)
		}

		// 队列应该为空
		length, _ := testRdb.LLen(ctx, prefixKeyQueue).Result()
		if length != 0 {
			t.Errorf("queue length after dequeue = %d, want 0", length)
		}

		// 验证 status 已被 Dequeue 设为 completed
		if dequeued.status != StatusCompleted {
			t.Errorf("status = %v, want %v", dequeued.status, StatusCompleted)
		}

		// 验证 Redis 里的 status 也更新了
		redisStatus, _ := testRdb.HGet(ctx, prefixKeyTask+original.id.String(), "status").Int()
		if TaskStatus(redisStatus) != StatusCompleted {
			t.Errorf("Redis status = %v, want %v", TaskStatus(redisStatus), StatusCompleted)
		}

		// 验证 taskSpec 数据完整往返
		if dequeued.Typename != "sms" {
			t.Errorf("typename = %q, want %q", dequeued.Typename, "sms")
		}
		if string(dequeued.Payload) != `{"phone":"123"}` {
			t.Errorf("payload = %q, want %q", string(dequeued.Payload), `{"phone":"123"}`)
		}

		// 验证 createdAt 在合理范围内
		now := time.Now()
		if dequeued.createdAt.After(now) {
			t.Errorf("createdAt %v is in the future", dequeued.createdAt)
		}
		if now.Sub(dequeued.createdAt) > 10*time.Second {
			t.Errorf("createdAt %v is too old", dequeued.createdAt)
		}
	})

	t.Run("FIFO order: first in, first out", func(t *testing.T) {
		s := setupStorer(t)
		tasks := []*Task{
			newTestTask("type-a", []byte("a")),
			newTestTask("type-b", []byte("b")),
			newTestTask("type-c", []byte("c")),
		}
		// LPush 到队列，所以出队顺序和入队顺序相反（stack 行为）
		// 除非用 LPop 出队...等一下，Dequeue 用的是 RPop
		// LPush + RPop = FIFO
		for _, task := range tasks {
			s.Enqueue(ctx, task)
		}

		var dequeued []uuid.UUID
		for range tasks {
			var t2 Task
			id, err := s.Dequeue(ctx, &t2)
			if err != nil {
				t.Fatalf("Dequeue() error = %v", err)
			}
			dequeued = append(dequeued, id)
		}

		// FIFO: 第一个入队的应该第一个出队
		for i, task := range tasks {
			if dequeued[i] != task.id {
				t.Errorf("position %d: got %s, want %s", i, dequeued[i], task.id)
			}
		}
	})
}
