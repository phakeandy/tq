# F3 延迟执行：forwardLoop 学习笔记

> 用途：你在写 `forwardLoop` 之前的参考资料。只讲概念和考察点，不写实现代码——代码请自己动手。

## 1. 你在实现什么

一句话：**让 `WithDelay(d)` 的任务在"提交时刻 + d"之前绝不被 Worker 消费**。

数据流（现在已经有一半了）：

```
enqueue(WithDelay)  →  scheduled zset（score = 到期时间戳）
                          │
                          │  forward() ← 你已实现（rdb.go）
                          ▼
                      pending list
                          │
                          │  dequeue() ← 已有
                          ▼
                      Worker 执行
```

缺的一环：`forward()` 写好了，但**没有任何代码定期调用它**。`forwardLoop` 就是那个"闹钟"。

## 2. 面试考点：延迟队列的三种经典实现

| 方案 | 原理 | 优点 | 缺点 |
|------|------|------|------|
| **ZSet + 轮询/定时搬移**（本项目） | score=到期时间，定期把 score≤now 的成员搬进待执行队列 | 简单、可靠、天然持久化（Redis 里） | 轮询间隔决定调度精度上限；有空转成本 |
| **时间轮（TimeWheel）** | 内存中按时间片挂桶，ticker 每 tick 推进指针，到期的桶里的任务触发 | 插入 O(1)、精度高、空转少 | 纯内存：进程重启丢状态，通常要配合持久化（如落盘+重启重建） |
| **Keyspace Notification（key 过期事件）** | 给任务 key 设 TTL，订阅 `expired` 事件 | 实现最简单 | 过期事件**不保证及时可靠**（惰性删除+采样删除），任务可能被延迟很久——适合兜底，不适合主力 |

面试被追问"为什么不用时间轮？"的答法：本项目追求**可靠 + 简单**，状态全部在 Redis（天然持久化、天然多实例共享）；时间轮的内存态和持久化之间的一致性成本高于收益。精度方面 PRD N3 只要求秒级，轮询即可满足。

## 3. 你的设计问题清单（写代码前先回答，review 时对答案）

1. **多久搬一次？**
   - 固定间隔轮询 vs 读 zset 最早 score 算出"还要睡多久"再醒？
   - 背后考察：**调度精度**（间隔越大误差越大，N3 要求秒级）与**空转成本**（醒得越频繁越浪费）的权衡。
2. **谁来搬？**
   - 独立 goroutine vs Worker 在 dequeue 空时顺手搬？
   - 背后考察：职责分离；`Run` 的 goroutine 生命周期管理（现在 `Run` 只 spawn worker，加了 forwarder 后要等它退出吗？）。
3. **多实例并发搬会不会重复？**
   - 两个进程同时 forward，同一个任务会不会被搬两次？
   - 答案线索在你自己的 `rdb.go` forward 脚本注释里：`ZREM == 1`。复习它，说出**为什么它天然防重**、为什么能支撑多实例部署。
4. **没有延迟任务时空转怎么办？**
   - zset 全空时，你的循环会不会变成 busy loop 狂刷 Redis？
   - 另外注意：`ctx` 取消时循环要能**及时退出**——裸 `time.Sleep` 是等不到取消的（想想 `select` + `time.Timer`）。

## 4. 测试验收（TDD：先红后绿）

`TestRunDelayedTask`（写在 tq_test.go，参考已有 `TestRun` 的写法）至少要断言：

1. 任务最终被执行、状态变为 `completed`（现在跑必失败 = 红）；
2. **执行时刻 ≥ 提交时刻 + delay**——证明它没有提前执行（N2 正确性要求）。建议在 handler 里记录执行时间，断言 `execAt - submittedAt >= delay`。
3. 提交**无延迟**的任务仍然立即执行（回归，别把 F2 弄坏）。

提示：`Run` 是阻塞的，测试要 goroutine 里跑 + `cancel()` 收尾（照抄 `TestRun` 的模式即可）。

## 5. 完成定义（Definition of Done）

- [ ] 先有红的 `TestRunDelayedTask`，再有实现，最后变绿
- [ ] `Run` 里启动并等待 forwardLoop
- [ ] 能口头回答第 3 节全部 4 个问题
- [ ] 顺手清理：`tq.go` 里 `WithDelay` 的 `TODO(F3)` 注释已经过时了，实现完记得删掉

## 6. 写测试时发现的两个点

1. **不能复用 `enqueueTask` helper**：它断言任务进了 pending list，而延迟任务进的是 scheduled zset——这正是 F3 的契约，测试 helper 里就把它锁死了（`enqueueDelayedTask`）。
2. **时间精度坑（面试可讲）**：enqueue/forward 脚本都用 `Unix()`（秒）比较时间。`WithDelay(300ms)` 的任务很可能被截断成"同一秒"→ 走 pending 分支**立即执行**；就算走了 scheduled，也可能比到期时间**提前近 1 秒**执行。测试里用 delay=2s + 1s 容差规避。后续收尾建议全面换成 `UnixMilli()`（毫秒）——"发现秒级时间戳导致延迟任务提前执行，改成毫秒时间戳"是很加分的面试故事。注意：`forward` 的 now 和 zset 的 score 单位必须一致，混用会让所有任务立即到期。
