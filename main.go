package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	// 注册 handler
	RegisterHandler("hello", func(ctx context.Context, task *Task) error {
		fmt.Printf("[worker] executing task %s, payload: %s\n", task.ID, task.Payload)
		time.Sleep(2 * time.Second)
		fmt.Printf("[worker] task %s done\n", task.ID)
		return nil
	})

	rdb := getRDB()
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("failed to close redis connection: %v", err)
		}
	}()

	// 启动 Worker（带优雅关闭）
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	go RunWorker(ctx, rdb, 2)

	// 提交一个任务
	task := NewTask(Options{
		TaskType: "hello",
		Payload:  []byte(`"hello world"`),
	})
	if err := task.Submit(rdb); err != nil {
		log.Fatalf("submit task: %v", err)
	}
	fmt.Println("submitted:", task.ID)

	// 等一下看看结果
	time.Sleep(5 * time.Second)

	stored, err := getTask(rdb, task.ID)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("result: status=%s\n", stored.Status)
	}

	cancel() // 优雅关闭
	time.Sleep(1 * time.Second)
}
