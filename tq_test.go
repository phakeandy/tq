package tq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	tq "github.com/phakeandy/task-queue"
)

func TestRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sendEmail := func(ctx context.Context, task *tq.Task) error {
		var p struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := json.Unmarshal(task.Payload, &p); err != nil {
			return err
		}
		fmt.Printf("Send a email from: %v, to: %v\n", p.From, p.To)
		return nil
	}

	h := tq.H{
		"email:send": sendEmail,
	}

	err := go tq.Run(ctx, h)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}
