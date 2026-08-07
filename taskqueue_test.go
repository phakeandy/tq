package taskqueue_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/phakeandy/task-queue"
)

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
