# Task-Queue 核心架构图

## 1. 状态流转（当前实现）

```mermaid
stateDiagram-v2
    [*] --> waiting: NewTask() 提交
    waiting --> running: worker BRPOP 拉取
    running --> completed: handler 返回 nil
    running --> failed: handler 返回 error

    note right of waiting
        F3 延迟执行(已实现): WithDelay 任务入队先进 scheduled zset
        forwardLoop 每 1s 把到期任务搬进 pending，再被 worker 拉取
    end note

    note right of [*]
        F5 幂等投递(已实现): WithIdempotencyKey 任务在提交时去重，
        检查与建任务在同一个 Lua 脚本内原子完成(满足 N1 并发去重)。
        同 key 未完成(pending/running/scheduled/retry) → 拒绝(ErrDuplicateInFlight)；
        同 key 已完成(completed/failed) → 返回原 job 的结果/失败原因，不再建新任务。
        索引: tq:{qname}:unique:{key} → jobID，TTL 默认 1h(可配置，对应 F13 结果保留期)。
        Handler 结果随 markAsCompleted 写入 job hash 的 result 字段。
    end note

    note right of running
        F8 超时: ctx.WithTimeout 已传入 handler
        但 handler 不理会 ctx 时无法强制中止(未实现)
    end note

    note right of failed
        F4 重试(未实现): failed 应回到 waiting
        需 MaxRetries 计数 + 退避(zset 延迟队列)
    end note

    completed --> [*]
    failed --> [*]
```

## 2. 组件交互（一次任务的完整旅程）

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer(业务方)
    participant R as Redis
    participant W as Worker goroutine ×N
    participant H as Handler(注册表)

    Note over P,H: 启动期: RegisterHandler(type, fn)
    P->>P: NewTask() → Status=waiting
    P->>R: Submit: SET task:{id} + LPUSH task_queue
    W->>R: BRPOP task_queue (阻塞等待)
    R-->>W: taskID
    W->>R: GET task:{id}
    R-->>W: Task JSON
    W->>R: SET task:{id} Status=running
    Note over W,H: 5a: handlersMu.RLock 查 handler<br/>5b: WithTimeout(task.Timeout)
    W->>H: handler(taskCtx, task)
    alt err == nil
        H-->>W: nil
        W->>R: SET task:{id} Status=completed
    else err != nil
        H-->>W: error
        W->>R: SET task:{id} Status=failed
    end
    Note over W: 循环, 继续 BRPOP 下一个任务
```

## 3. 未实现路线（图上虚线部分对应的功能）

| 功能 | 依赖的基础设施 | 图上的位置 |
|------|---------------|-----------|
| F4 重试 | zset 延迟队列 + MaxRetries 计数 | failed → waiting 虚线 |
| F8 超时强制终止 | 超时后强制标 failed | running 状态 |
| F9 故障隔离 | handler panic recover | running 状态 |
| F11 可观测 | metrics 采集 | 全部状态 |
