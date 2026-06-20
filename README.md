# utils

`utils`는 engine2와 함께 쓰기 위한 공용 Zig 0.16 유틸리티 패키지다.

- `ThreadPool` / `Task` / `init` / `start` / `deinit` / `join` / `addTask` / `stopTask` 제공
  (Odin `core:thread.Pool` 1:1 매핑)
- `WorkerPool` — work-stealing 기반 저경합 풀 (`std.Thread.spawn` × `n_jobs`)
- `WaitGroup` — Go `sync.WaitGroup` 스타일 동기화 프리미티브
- `SpinLock` — atomic swap + `spinLoopHint` 기반 spinlock
- `SFL` — Segregated Free List 단일 스레드 할당자 (`std.mem.Allocator` 인터페이스 제공)
- `noOpWorker` — `worker_init`/`worker_fini` 기본 콜백

`ThreadPool` task queue는 Odin 원본처럼 FIFO ring-buffer이며, `std.Io.Mutex` + `std.Io.Condition`으로 보호한다.
`WorkerPool`은 워커별 bounded atomic queue를 두고, 자기 queue가 비면 다른 워커 queue에서 steal한다.
`WorkerPool`의 task push/pop/steal 경로는 mutex를 잡지 않고, condition은 idle worker wakeup에만 사용한다.
shutdown flag는 `std.atomic.Value(bool)`로 관리한다.

## 구현 선택

- `ThreadPool`: Odin `core:thread.Pool` 1:1 매핑이 필요하거나 단순한 FIFO global queue 동작이 필요할 때 사용하면 된다.
- `WorkerPool`: 작은 작업이 아주 많아서 global queue lock 경쟁이나 queue 비용이 작업 비용보다 커질 수 있을 때 사용하면 된다.

## ThreadPool 사용법

```zig
const utils = @import("utils");

var pool = utils.ThreadPool.init(
    allocator,
    n_jobs,
    utils.noOpWorker, null,
    utils.noOpWorker, null,
);
try pool.start();
defer pool.deinit();

pool.addTask(.{ .proc = myTask, .data = null });
```

## WorkerPool 사용법

```zig
const utils = @import("utils");

var pool = utils.WorkerPool.init(
    allocator,
    n_jobs,
    utils.noOpWorker, null,
    utils.noOpWorker, null,
);
try pool.start();
defer pool.deinit();

pool.addTask(.{ .proc = myTask, .data = null });
```

queue 크기를 직접 정하려면 `initWithOptions`를 사용하면 된다.

```zig
var pool = utils.WorkerPool.initWithOptions(
    allocator,
    n_jobs,
    utils.noOpWorker, null,
    utils.noOpWorker, null,
    .{ .queue_capacity = 1024 },
);
```

`WorkerPool` queue는 bounded라서, 꽉 찬 상황을 호출자가 알아야 하면 `tryAddTask`를 사용하면 된다.

```zig
if (!pool.tryAddTask(.{ .proc = myTask, .data = null })) {
    // 모든 worker queue가 가득 찬 상태
}
```

## WaitGroup 사용법

`WaitGroup`은 Go의 `sync.WaitGroup`과 동일한 패턴으로, 다수의 비동기 작업 완료를 대기하는 동기화 프리미티브다.

- `add(n)`: 내부 카운터를 n만큼 증가시킨다. 작업을 시작하기 전에 호출한다.
- `done()`: 카운터를 1 감소. 작업 완료 시 `defer wg.done()` 형태로 사용한다.
- `wait()`: 카운터가 0이 될 때까지 호출자를 블록한다.

초기화는 필드 기본값을 그대로 사용하므로 `utils.WaitGroup{}`으로 생성하면 된다.

### ThreadPool + WaitGroup

```zig
const utils = @import("utils");

var wg = utils.WaitGroup{};
var pool = utils.ThreadPool.init(
    allocator,
    n_jobs,
    utils.noOpWorker, null,
    utils.noOpWorker, null,
);
try pool.start();
defer pool.deinit();

const task_count = 10;
wg.add(task_count);
for (0..task_count) |_| {
    pool.addTask(.{ .proc = myTask, .data = &wg });
}
wg.wait(); // 모든 task 완료 시점
```

```zig
fn myTask(data: ?*anyopaque) void {
    const wg: *utils.WaitGroup = @ptrCast(@alignCast(data));
    defer wg.done();
    // 실제 작업 수행
}
```

### WorkerPool + WaitGroup

```zig
const utils = @import("utils");

var wg = utils.WaitGroup{};
var pool = utils.WorkerPool.init(
    allocator,
    n_jobs,
    utils.noOpWorker, null,
    utils.noOpWorker, null,
);
try pool.start();
defer pool.deinit();

const task_count = 10;
wg.add(task_count);
for (0..task_count) |_| {
    pool.addTask(.{ .proc = myTask, .data = &wg });
}
wg.wait();
```

## SpinLock 사용법

```zig
const utils = @import("utils");

var lock: utils.SpinLock = .{};
lock.lock();
defer lock.unlock();
// critical section
```

## SFL 사용법

```zig
const utils = @import("utils");

var g: utils.SFL = .{};
if (!g.init(4096)) return;
defer g.destroy();

const al = g.allocator();
const data = try al.alloc(u8, 128);
defer al.free(data);
```

## 빌드 / 테스트

```sh
zig build
zig build test
```
