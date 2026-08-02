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
			log.Printf("failed to close redis connection: %w", err)
		}
	}()

	// 启动 Worker（带优雅关闭）
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	go RunWorker(ctx, rdb, 2)

	// 提交一个任务
	id, err := Submit(rdb, SubmitRequest{
		TaskType: "hello",
		Payload:  []byte(`"hello world"`),
	})
	fmt.Println("submitted:", id, err)

	// 等一下看看结果
	time.Sleep(5 * time.Second)

	task, err := GetTask(rdb, id)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("result: status=%s\n", task.Status)
	}

	cancel() // 优雅关闭
	time.Sleep(1 * time.Second)
}
