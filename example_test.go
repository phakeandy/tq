package tq_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	tq "github.com/phakeandy/task-queue"
)

func ExampleRunWorker() {
	// 注册 handler
	tq.RegisterHandler("hello", func(ctx context.Context, task *tq.Task) error {
		fmt.Printf("[worker] executing task, payload: %s\n", task.Payload)
		time.Sleep(2 * time.Second)
		fmt.Println("[worker] task done")
		return nil
	})

	rdb := tq.NewRDB()
	defer func() {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis connection", "err", err)
		}
	}()

	// 启动 Worker（带优雅关闭）
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	go tq.RunWorker(ctx, rdb, 2)

	// 提交一个任务
	task, err := tq.NewTask(tq.Options{
		TaskType: "hello",
		Payload:  []byte(`"hello world"`),
	})
	if err != nil {
		slog.Error("new task", "err", err)
		os.Exit(1)
	}
	if err := task.Submit(rdb); err != nil {
		slog.Error("submit task", "err", err)
		os.Exit(1)
	}
	fmt.Println("submitted")

	// 轮询直到任务到达终态（completed/failed）
	var stored *tq.Task
	deadline := time.Now().Add(10 * time.Second)
	for {
		stored, err = tq.GetTask(rdb, task.ID)
		if err == nil && stored.Status != tq.StatusWaiting && stored.Status != tq.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			slog.Error("timed out waiting for task")
			os.Exit(1)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("result: status=%s\n", stored.Status)
	}

	cancel() // 优雅关闭
	time.Sleep(1 * time.Second)

	// Unordered output:
	// [worker] executing task, payload: "hello world"
	// [worker] task done
	// submitted
	// result: status=completed
}
