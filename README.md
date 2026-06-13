# thread_pool

`thread_pool`은 Odin `core:thread.Pool`의 Zig 0.16 1:1 패키지(`ThreadPool`)와 저경합 work-stealing 버전(`WorkerPool`)을 제공합니다.

- `ThreadPool` / `Task` / `init` / `start` / `deinit` / `join` / `addTask` / `stopTask` 제공
- `std.Thread.spawn` × `n_jobs`로 워커를 구성
- `ThreadPool` task queue는 `std.Io.Mutex` + `std.Io.Condition`으로 보호
- `WorkerPool`은 워커별 bounded atomic queue를 두고, 자기 queue가 비면 다른 워커 queue에서 steal
- `WorkerPool`의 task push/pop/steal 경로는 mutex를 잡지 않고, condition은 idle worker wakeup에만 사용
- shutdown flag는 `std.atomic.Value(bool)`로 관리

## 구현 선택

- `ThreadPool`: Odin `core:thread.Pool` 1:1 매핑이 필요하거나 단순한 FIFO global queue 동작이 필요할 때 사용하면 됩니다.
- `WorkerPool`: 작은 작업이 아주 많아서 global queue lock 경쟁이나 queue 비용이 작업 비용보다 커질 수 있을 때 사용하면 됩니다.

## ThreadPool 사용법

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

## WorkerPool 사용법

```zig
const thread_pool = @import("thread_pool");

var pool = thread_pool.WorkerPool.init(
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

queue 크기를 직접 정하려면 `initWithOptions`를 사용하면 됩니다.

```zig
var pool = thread_pool.WorkerPool.initWithOptions(
    allocator,
    n_jobs,
    worker_init,
    worker_init_data,
    worker_fini,
    worker_fini_data,
    .{ .queue_capacity = 1024 },
);
```

`WorkerPool` queue는 bounded라서, 꽉 찬 상황을 호출자가 알아야 하면 `tryAddTask`를 사용하면 됩니다.

```zig
if (!pool.tryAddTask(.{ .proc = myTask, .data = null })) {
    // 모든 worker queue가 가득 찬 상태
}
```

## 빌드 / 테스트

```sh
zig build
zig build test
```
