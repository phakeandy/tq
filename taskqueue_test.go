package taskqueue_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	taskqueue "github.com/phakeandy/task-queue"
	"github.com/redis/go-redis/v9"
)

var testRdb redis.UniversalClient

func TestMain(m *testing.M) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6380"
	}

	testRdb = redis.NewClient(&redis.Options{Addr: addr})
	defer testRdb.Close() //nolint

	ctx := context.Background()
	if err := testRdb.Ping(ctx).Err(); err != nil {
		panic("cannot connect to Redis at " + addr + ": " + err.Error())
	}

	code := m.Run()
	os.Exit(code)
}

func TestNewTask(t *testing.T) {
	type greet struct {
		From string `json:"from"`
		To   string `json:"to"`
	}

	payload, err := json.Marshal(greet{From: "alice", To: "bob"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	task, err := taskqueue.NewTask(
		"greeting",
		payload,
		taskqueue.WithMaxRetries(0),
		taskqueue.WithIdempotencyKey("order-12345"),
		taskqueue.WithDelay(5*time.Second),
		taskqueue.WithTimeout(60*time.Second),
	)
	t.Logf("Task is %+v", task)
	if err != nil {
		t.Fatalf("NewTask: %v", err)
	}
	if task == nil {
		t.Fatal("NewTask returned a nil task")
	}
}

func TestConsumer_Start(t *testing.T) {
	s := taskqueue.NewStorer(testRdb)
	c, err := taskqueue.NewConsumer(s, 10)
	if err != nil {
		t.Errorf("new consumer fail %v", err)
	}
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Errorf("start consumer fail %v", err)
	}

}
