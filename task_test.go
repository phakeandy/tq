package tq_test

import (
	"context"
	"testing"
	"time"

	tq "github.com/phakeandy/task-queue"
)

func TestTaskLifeCycle(t *testing.T) {
	ctx := context.Background()
	rdb := tq.NewRDB()
	defer rdb.Close()

	// 前提：Redis 必须可用。不可用就跳过（t.Skip），
	// 这样没装 Redis 的人 clone 下来 go test 也能通过。
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available, skipping: %v", err)
	}
	// 清空队列：task_queue 是全局 key，避免历史任务干扰本测试。
	if err := rdb.Del(ctx, "task_queue").Err(); err != nil {
		t.Fatalf("clear queue: %v", err)
	}

	// ---- 阶段一：提交（此时 worker 未启动，状态断言是确定的）----
	task, err := tq.NewTask(tq.Options{
		TaskType: "hello",
		Payload:  []byte(`"hi"`),
	})
	if err != nil {
		t.Fatalf("NewTask: %v", err)
	}
	// 顺手验证 NewTask 的默认值
	if task.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", task.MaxRetries)
	}
	if task.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", task.Timeout)
	}
	if task.Status != tq.StatusWaiting {
		t.Errorf("Status = %q, want %q", task.Status, tq.StatusWaiting)
	}

	if err := task.Submit(rdb); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// 提交后、消费前：状态必须是 waiting。
	// 这里没有竞态，因为 worker 还没启动。
	stored, err := tq.GetTask(rdb, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if stored.Status != tq.StatusWaiting {
		t.Errorf("status after submit = %q, want %q", stored.Status, tq.StatusWaiting)
	}

	// ---- 阶段二：启动 worker 消费 ----
	tq.RegisterHandler("hello", func(ctx context.Context, _ *tq.Task) error {
		return nil
	})
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go tq.RunWorker(workerCtx, rdb, 2)

	// ---- 阶段三：轮询直到终态 ----
	// 为什么轮询 + deadline，而不是 time.Sleep(5s) 后断言？
	// 任务快时 50ms 就完成了，Sleep 浪费 4.95s；任务卡住时 Sleep 也发现不了，
	// 只会等满 5s 得到一个 "status=running" 的失败信息。轮询两者都更好。
	deadline := time.Now().Add(5 * time.Second)
	for {
		stored, err = tq.GetTask(rdb, task.ID)
		if err != nil {
			t.Fatalf("GetTask: %v", err)
		}
		if stored.Status == tq.StatusCompleted || stored.Status == tq.StatusFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task stuck in %q, want terminal status", stored.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if stored.Status != tq.StatusCompleted {
		t.Errorf("final status = %q, want %q", stored.Status, tq.StatusCompleted)
	}
}
