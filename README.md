# thread_pool

`thread_pool`은 Odin `core:thread.Pool`의 Zig 0.16 1:1 패키지(ThreadPool) + (완성되고 채음) 입니다.

- `ThreadPool` / `Task` / `init` / `start` / `deinit` / `join` / `addTask` / `stopTask` 제공
- `std.Thread.spawn` × `n_jobs`로 워커를 구성
- task queue는 `std.Io.Mutex` + `std.Io.Condition`으로 보호
- shutdown flag는 `std.atomic.Value(bool)`로 관리

## 사용법

```zig
const thread_pool = @import("thread_pool");

var pool = thread_pool.ThreadPool.init(
    allocator,
    n_jobs,
    worker_init,
    worker_init_data,
    worker_fini,
    worker_fini_data,
);
try pool.start();
defer pool.deinit();

pool.addTask(.{ .proc = myTask, .data = null });
```

## 빌드 / 테스트

```sh
zig build
zig build test
```
