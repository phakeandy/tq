# sync.Map: when to use, and why not here

## What it is
Two maps inside: `read` (atomic load, lock-free reads) + `dirty` (mutex-guarded writes, promoted periodically). Designed for lock-free reads on read-heavy workloads.

## When to use it (official target scenarios)
- A key written once, read many times (caches that only grow)
- Multiple goroutines read/write **disjoint** key sets
- You need compound atomic ops: `LoadOrStore`, `CompareAndSwap`

## Why NOT here (handlers registry)
- Go docs: *"Map is specialized. Most code should use a plain map + mutex/RWMutex."*
- Clunky API: `Load` returns `any` -> type assertion; plain map gives compile-time type safety via indexing
- No perf difference at this scale (register once, one lookup per task). RWMutex read path is ~an atomic op in uncontended case
- Explicit `RLock`/`RUnlock` makes the concurrency boundary visible in review; sync.Map's safety is implicit magic
- No compound ops needed here

## Bonus
Lock could even be dropped if "RegisterHandler before RunWorker" contract were trusted (happens-before via goroutine start). Mutex is cheap defensive insurance against contract violations.
