package tq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/phakeandy/tq"
	"github.com/redis/go-redis/v9"
)

func TestRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	r := tq.NewRDB(client)

	sendEmail := func(ctx context.Context, job *tq.Job) error {
		var p struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return err
		}
		fmt.Printf("Send a email from: %v, to: %v\n", p.From, p.To)
		return nil
	}

	h := tq.H{
		"email:send": sendEmail,
	}

	err := tq.Run(ctx, r, h, 3)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}
