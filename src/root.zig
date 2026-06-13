//! thread_pool — Odin `core:thread.Pool` 1:1 매핑.
//!
//! ## 사용법
//!
//! ```zig
//! const thread_pool = @import("thread_pool");
//!
//! var pool = thread_pool.ThreadPool.init(
//!     allocator,
//!     n_jobs,
//!     worker_init, worker_init_data,
//!     worker_fini, worker_fini_data,
//! );
//! try pool.start();
//! defer pool.deinit();
//!
//! pool.addTask(.{ .proc = myTask, .data = null });
//! ```
//!
//! ## API 매핑 (Odin 1:1)
//!
//! | Odin | Zig |
//! |:---|:---|
//! | `thread.Pool` | `ThreadPool` |
//! | `Task :: struct { proc, data }` | `Task` |
//! | `thread.pool_init` | `ThreadPool.init` |
//! | `thread.pool_start` | `ThreadPool.start` |
//! | `thread.pool_destroy` | `ThreadPool.deinit` |
//! | `thread.pool_join` | `ThreadPool.join` |
//! | `thread.pool_add_task` | `ThreadPool.addTask` |
//! | `thread.pool_stop_task` | `ThreadPool.stopTask` |
//!
//! ## 동기화 프리미티브
//!
//! Zig 0.16의 `std.Thread`에는 `Mutex`/`Condition`이 없으므로 `std.Io.Mutex`/`std.Io.Condition` 사용.
//! mutex + atomic.Value + condition variable 조합으로 Odin 동작 재현.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Task = struct {
    proc: *const fn (data: ?*anyopaque) void,
    data: ?*anyopaque = null,
};

pub const ThreadPool = struct {
    allocator: Allocator,
    n_jobs: usize,
    /// 각 워커 시작 시 호출 (Odin `init_proc` 1:1)
    worker_init: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_init_data: ?*anyopaque = null,
    /// 각 워커 종료 시 호출 (Odin `fini_proc` 1:1)
    worker_fini: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_fini_data: ?*anyopaque = null,

    /// 실제 스폰된 워커 핸들
    workers: ?[]std.Thread = null,
    /// task 큐 (FIFO). mutex로 보호.
    tasks: std.ArrayList(Task) = .empty,
    /// worker 사이 mutex.
    ///
    /// Zig 0.16의 조건 변수는 `std.Io.Condition`이며, 같은 Io mutex와 함께 사용한다.
    mutex: std.Io.Mutex = .init,
    /// worker 깨우기용 condition variable.
    cond: std.Io.Condition = .init,
    /// Io vtable. futex 기반 mutex/condition operation에 사용.
    io_impl: std.Io.Threaded = .init_single_threaded,
    /// shutdown 플래그
    shutdown: std.atomic.Value(bool) = .init(false),
    /// 시작 여부
    started: bool = false,

    /// Odin `thread.pool_init` 1:1.
    pub fn init(
        allocator: Allocator,
        n_jobs: usize,
        worker_init: *const fn (data: ?*anyopaque) void,
        worker_init_data: ?*anyopaque,
        worker_fini: *const fn (data: ?*anyopaque) void,
        worker_fini_data: ?*anyopaque,
    ) ThreadPool {
        return .{
            .allocator = allocator,
            .n_jobs = n_jobs,
            .worker_init = worker_init,
            .worker_init_data = worker_init_data,
            .worker_fini = worker_fini,
            .worker_fini_data = worker_fini_data,
            .io_impl = std.Io.Threaded.init(allocator, .{}),
        };
    }

    /// Odin `thread.pool_destroy` 1:1.
    pub fn deinit(self: *ThreadPool) void {
        if (self.started) {
            self.join();
        }
        self.tasks.deinit(self.allocator);
        self.io_impl.deinit();
    }

    /// Odin `thread.pool_start` 1:1 — n_jobs 개수만큼 워커 spawn.
    pub fn start(self: *ThreadPool) !void {
        if (self.started) return;
        if (self.n_jobs == 0) {
            self.started = true;
            return;
        }

        const workers = try self.allocator.alloc(std.Thread, self.n_jobs);
        errdefer self.allocator.free(workers);

        var spawned: usize = 0;
        errdefer {
            self.shutdown.store(true, .release);
            self.mutex.lockUncancelable(self.io());
            self.cond.broadcast(self.io());
            self.mutex.unlock(self.io());

            for (workers[0..spawned]) |w| w.join();
            self.allocator.free(workers);
        }

        for (workers) |*w| {
            w.* = try std.Thread.spawn(.{}, workerLoop, .{self});
            spawned += 1;
        }

        self.workers = workers[0..spawned];
        self.started = true;
    }

    /// Odin `thread.pool_join` 1:1 — 모든 워커 종료 대기.
    pub fn join(self: *ThreadPool) void {
        if (!self.started) return;
        if (self.workers) |workers| {
            // 워커 깨우기: shutdown 플래그 set + cond broadcast.
            self.shutdown.store(true, .release);
            self.mutex.lockUncancelable(self.io());
            self.cond.broadcast(self.io());
            self.mutex.unlock(self.io());

            for (workers) |w| w.join();
            self.allocator.free(workers);
            self.workers = null;
            self.tasks.clearRetainingCapacity();
            self.shutdown.store(false, .release);
        }
        self.started = false;
    }

    /// Odin `thread.pool_add_task` 1:1 — task 큐에 push + 워커 깨움.
    pub fn addTask(self: *ThreadPool, task: Task) void {
        if (self.n_jobs == 0 or !self.started or self.shutdown.load(.acquire)) return;

        self.mutex.lockUncancelable(self.io());
        defer self.mutex.unlock(self.io());
        self.tasks.append(self.allocator, task) catch return;
        self.cond.signal(self.io());
    }

    /// Odin `thread.pool_stop_task` 1:1 — graceful stop.
    /// Odin 동작: stop_task도 일반 task처럼 큐에 push하고, 해당 task proc이 stop 신호로 활용됨.
    /// 여기서는 task를 큐에 push해서 워커를 깨운다.
    pub fn stopTask(self: *ThreadPool, task: Task) void {
        self.addTask(task);
    }

    fn io(self: *ThreadPool) std.Io {
        return self.io_impl.io();
    }
};

/// 워커 스레드 메인 루프.
fn workerLoop(pool: *ThreadPool) void {
    // 1. worker_init 호출 (Odin `init_proc` 1:1)
    pool.worker_init(pool.worker_init_data);
    defer pool.worker_fini(pool.worker_fini_data);

    pool.mutex.lockUncancelable(pool.io());
    defer pool.mutex.unlock(pool.io());

    while (true) {
        // shutdown 플래그 체크 (Odin `__exiting` 매핑)
        if (pool.shutdown.load(.acquire)) {
            // 큐에 남은 task는 drain하지 않고 즉시 종료 (Odin 동작)
            return;
        }

        if (pool.tasks.items.len > 0) {
            const task = pool.tasks.orderedRemove(0);

            // task 실행 중에는 mutex 풀고, 끝나면 다시 lock.
            pool.mutex.unlock(pool.io());
            task.proc(task.data);
            pool.mutex.lockUncancelable(pool.io());
        } else {
            // 큐가 비어있으면 condvar로 대기.
            pool.cond.waitUncancelable(pool.io(), &pool.mutex);
        }
    }
}

pub const WorkerPoolOptions = struct {
    queue_capacity: usize = DefaultWorkerPoolQueueCapacity,
};

pub const DefaultWorkerPoolQueueCapacity: usize = 256;

pub const WorkerPool = struct {
    allocator: Allocator,
    n_jobs: usize,
    queue_capacity: usize = DefaultWorkerPoolQueueCapacity,
    /// 각 워커 시작 시 호출.
    worker_init: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_init_data: ?*anyopaque = null,
    /// 각 워커 종료 시 호출.
    worker_fini: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_fini_data: ?*anyopaque = null,

    /// 실제 스폰된 워커 핸들.
    workers: ?[]std.Thread = null,
    /// 워커별 bounded atomic queue.
    queues: ?[]AtomicTaskQueue = null,
    /// 워커에 넘기는 고정 context.
    contexts: ?[]WorkerPoolContext = null,
    /// idle worker sleep/wakeup 전용 mutex.
    wake_mutex: std.Io.Mutex = .init,
    /// idle worker wakeup 전용 condition variable.
    wake_cond: std.Io.Condition = .init,
    /// Io vtable. futex 기반 mutex/condition operation에 사용.
    io_impl: std.Io.Threaded = .init_single_threaded,
    /// shutdown 플래그.
    shutdown: std.atomic.Value(bool) = .init(false),
    /// addTask가 시작할 queue 위치. enqueue 분산용.
    submit_cursor: std.atomic.Value(usize) = .init(0),
    /// sleep 중인 worker 수. wakeup signal 생략 여부 판단에만 사용.
    idle_workers: std.atomic.Value(usize) = .init(0),
    /// 테스트와 관찰용: 다른 worker queue에서 가져온 task 수.
    steal_count: std.atomic.Value(usize) = .init(0),
    /// 시작 여부.
    started: bool = false,

    pub fn init(
        allocator: Allocator,
        n_jobs: usize,
        worker_init: *const fn (data: ?*anyopaque) void,
        worker_init_data: ?*anyopaque,
        worker_fini: *const fn (data: ?*anyopaque) void,
        worker_fini_data: ?*anyopaque,
    ) WorkerPool {
        return initWithOptions(
            allocator,
            n_jobs,
            worker_init,
            worker_init_data,
            worker_fini,
            worker_fini_data,
            .{},
        );
    }

    pub fn initWithOptions(
        allocator: Allocator,
        n_jobs: usize,
        worker_init: *const fn (data: ?*anyopaque) void,
        worker_init_data: ?*anyopaque,
        worker_fini: *const fn (data: ?*anyopaque) void,
        worker_fini_data: ?*anyopaque,
        options: WorkerPoolOptions,
    ) WorkerPool {
        return .{
            .allocator = allocator,
            .n_jobs = n_jobs,
            .queue_capacity = options.queue_capacity,
            .worker_init = worker_init,
            .worker_init_data = worker_init_data,
            .worker_fini = worker_fini,
            .worker_fini_data = worker_fini_data,
            .io_impl = std.Io.Threaded.init(allocator, .{}),
        };
    }

    pub fn deinit(self: *WorkerPool) void {
        if (self.started) {
            self.join();
        }
        self.deinitQueues();
        self.io_impl.deinit();
    }

    pub fn start(self: *WorkerPool) !void {
        if (self.started) return;
        if (self.n_jobs == 0) {
            self.started = true;
            return;
        }
        if (self.queue_capacity == 0) return error.InvalidQueueCapacity;

        const queues = try self.allocator.alloc(AtomicTaskQueue, self.n_jobs);
        errdefer self.allocator.free(queues);

        var queue_count: usize = 0;
        errdefer {
            for (queues[0..queue_count]) |*queue| queue.deinit(self.allocator);
        }
        for (queues) |*queue| {
            queue.* = try AtomicTaskQueue.init(self.allocator, self.queue_capacity);
            queue_count += 1;
        }

        const contexts = try self.allocator.alloc(WorkerPoolContext, self.n_jobs);
        errdefer self.allocator.free(contexts);

        const workers = try self.allocator.alloc(std.Thread, self.n_jobs);
        errdefer self.allocator.free(workers);

        self.queues = queues;
        self.contexts = contexts;
        self.workers = workers;
        self.shutdown.store(false, .release);
        self.submit_cursor.store(0, .release);
        self.idle_workers.store(0, .release);
        self.steal_count.store(0, .release);

        var spawned: usize = 0;
        errdefer {
            self.shutdown.store(true, .release);
            self.wakeAll();
            for (workers[0..spawned]) |w| w.join();
            self.workers = null;
            self.contexts = null;
            self.queues = null;
        }

        for (workers, 0..) |*worker, index| {
            contexts[index] = .{ .pool = self, .index = index };
            worker.* = try std.Thread.spawn(.{}, workerPoolLoop, .{&contexts[index]});
            spawned += 1;
        }

        self.workers = workers[0..spawned];
        self.started = true;
    }

    pub fn join(self: *WorkerPool) void {
        if (!self.started) return;
        if (self.workers) |workers| {
            self.shutdown.store(true, .release);
            self.wakeAll();

            for (workers) |worker| worker.join();
            self.allocator.free(workers);
            self.workers = null;
        }
        if (self.contexts) |contexts| {
            self.allocator.free(contexts);
            self.contexts = null;
        }
        self.deinitQueues();
        self.shutdown.store(false, .release);
        self.idle_workers.store(0, .release);
        self.started = false;
    }

    pub fn addTask(self: *WorkerPool, task: Task) void {
        _ = self.tryAddTask(task);
    }

    pub fn stopTask(self: *WorkerPool, task: Task) void {
        self.addTask(task);
    }

    pub fn tryAddTask(self: *WorkerPool, task: Task) bool {
        if (self.n_jobs == 0 or !self.started or self.shutdown.load(.acquire)) return false;
        const queues = self.queues orelse return false;
        if (queues.len == 0) return false;

        const start_index = self.submit_cursor.fetchAdd(1, .monotonic) % queues.len;
        var offset: usize = 0;
        while (offset < queues.len) : (offset += 1) {
            const queue_index = (start_index + offset) % queues.len;
            if (queues[queue_index].tryPush(task)) {
                self.wakeOne();
                return true;
            }
        }
        return false;
    }

    fn deinitQueues(self: *WorkerPool) void {
        if (self.queues) |queues| {
            for (queues) |*queue| queue.deinit(self.allocator);
            self.allocator.free(queues);
            self.queues = null;
        }
    }

    fn wakeOne(self: *WorkerPool) void {
        if (self.idle_workers.load(.acquire) == 0) return;
        self.wake_mutex.lockUncancelable(self.io());
        self.wake_cond.signal(self.io());
        self.wake_mutex.unlock(self.io());
    }

    fn wakeAll(self: *WorkerPool) void {
        self.wake_mutex.lockUncancelable(self.io());
        self.wake_cond.broadcast(self.io());
        self.wake_mutex.unlock(self.io());
    }

    fn findTask(self: *WorkerPool, worker_index: usize) ?Task {
        const queues = self.queues orelse return null;
        if (queues.len == 0) return null;

        if (queues[worker_index].tryPop()) |task| {
            return task;
        }

        var offset: usize = 1;
        while (offset < queues.len) : (offset += 1) {
            const queue_index = (worker_index + offset) % queues.len;
            if (queues[queue_index].tryPop()) |task| {
                _ = self.steal_count.fetchAdd(1, .monotonic);
                return task;
            }
        }
        return null;
    }

    fn io(self: *WorkerPool) std.Io {
        return self.io_impl.io();
    }
};

const WorkerPoolContext = struct {
    pool: *WorkerPool,
    index: usize,
};

const AtomicTaskQueue = struct {
    slots: []QueueSlot,
    head: std.atomic.Value(usize) = .init(0),
    tail: std.atomic.Value(usize) = .init(0),

    fn init(allocator: Allocator, capacity: usize) !AtomicTaskQueue {
        const slots = try allocator.alloc(QueueSlot, capacity);
        for (slots, 0..) |*slot, index| {
            slot.* = .{
                .sequence = .init(index),
                .task = emptyTask(),
            };
        }
        return .{ .slots = slots };
    }

    fn deinit(self: *AtomicTaskQueue, allocator: Allocator) void {
        allocator.free(self.slots);
        self.* = .{ .slots = &.{} };
    }

    fn tryPush(self: *AtomicTaskQueue, task: Task) bool {
        const capacity = self.slots.len;
        var position = self.tail.load(.acquire);
        while (true) {
            if (position - self.head.load(.acquire) >= capacity) {
                return false;
            }
            const slot = &self.slots[position % capacity];
            const sequence = slot.sequence.load(.acquire);
            if (sequence == position) {
                if (self.tail.cmpxchgWeak(position, position + 1, .acq_rel, .acquire) == null) {
                    slot.task = task;
                    slot.sequence.store(position + 1, .release);
                    return true;
                }
                position = self.tail.load(.acquire);
            } else if (sequence < position) {
                return false;
            } else {
                position = self.tail.load(.acquire);
            }
        }
    }

    fn tryPop(self: *AtomicTaskQueue) ?Task {
        const capacity = self.slots.len;
        var position = self.head.load(.acquire);
        while (true) {
            const slot = &self.slots[position % capacity];
            const expected_sequence = position + 1;
            const sequence = slot.sequence.load(.acquire);
            if (sequence == expected_sequence) {
                if (self.head.cmpxchgWeak(position, position + 1, .acq_rel, .acquire) == null) {
                    const task = slot.task;
                    slot.sequence.store(position + capacity, .release);
                    return task;
                }
                position = self.head.load(.acquire);
            } else if (sequence < expected_sequence) {
                return null;
            } else {
                position = self.head.load(.acquire);
            }
        }
    }
};

const QueueSlot = struct {
    sequence: std.atomic.Value(usize),
    task: Task,
};

fn workerPoolLoop(context: *WorkerPoolContext) void {
    const pool = context.pool;
    const worker_index = context.index;

    pool.worker_init(pool.worker_init_data);
    defer pool.worker_fini(pool.worker_fini_data);

    while (true) {
        if (pool.shutdown.load(.acquire)) return;

        if (pool.findTask(worker_index)) |task| {
            task.proc(task.data);
            continue;
        }

        _ = pool.idle_workers.fetchAdd(1, .acq_rel);
        pool.wake_mutex.lockUncancelable(pool.io());
        if (pool.shutdown.load(.acquire)) {
            pool.wake_mutex.unlock(pool.io());
            _ = pool.idle_workers.fetchSub(1, .acq_rel);
            return;
        }
        if (pool.findTask(worker_index)) |task| {
            pool.wake_mutex.unlock(pool.io());
            _ = pool.idle_workers.fetchSub(1, .acq_rel);
            task.proc(task.data);
            continue;
        }
        pool.wake_cond.waitUncancelable(pool.io(), &pool.wake_mutex);
        pool.wake_mutex.unlock(pool.io());
        _ = pool.idle_workers.fetchSub(1, .acq_rel);
    }
}

fn emptyTask() Task {
    return .{ .proc = &noOpWorker, .data = null };
}

pub fn noOpWorker(_: ?*anyopaque) void {}

var g_test_worker_init_count: std.atomic.Value(usize) = .init(0);
var g_test_worker_fini_count: std.atomic.Value(usize) = .init(0);
var g_test_counter: std.atomic.Value(usize) = .init(0);
var g_test_block_worker_init: std.atomic.Value(bool) = .init(false);

fn testWorkerInit(data: ?*anyopaque) void {
    _ = data;
    _ = g_test_worker_init_count.fetchAdd(1, .monotonic);
}

fn testWorkerFini(data: ?*anyopaque) void {
    _ = data;
    _ = g_test_worker_fini_count.fetchAdd(1, .monotonic);
}

fn testTask(_: ?*anyopaque) void {
    _ = g_test_counter.fetchAdd(1, .monotonic);
}

fn slowTestTask(_: ?*anyopaque) void {
    for (0..256) |_| {
        std.Thread.yield() catch {};
    }
    _ = g_test_counter.fetchAdd(1, .monotonic);
}

fn blockingWorkerInit(data: ?*anyopaque) void {
    _ = data;
    _ = g_test_worker_init_count.fetchAdd(1, .monotonic);
    while (g_test_block_worker_init.load(.acquire)) {
        std.atomic.spinLoopHint();
    }
}

const ProducerSubmitContext = struct {
    pool: *WorkerPool,
    count: usize,
};

fn producerSubmitLoop(context: *ProducerSubmitContext) void {
    var submitted: usize = 0;
    while (submitted < context.count) {
        if (context.pool.tryAddTask(.{ .proc = &testTask, .data = null })) {
            submitted += 1;
        } else {
            std.Thread.yield() catch {};
        }
    }
}

fn waitForCounter(counter: *std.atomic.Value(usize), expected: usize) !void {
    var attempts: usize = 0;
    while (counter.load(.monotonic) < expected) : (attempts += 1) {
        try std.Thread.yield();
        if (attempts > 1_000_000) break;
    }
    try std.testing.expectEqual(expected, counter.load(.monotonic));
}

test "ThreadPool init stores config" {
    var pool = ThreadPool.init(
        std.heap.page_allocator,
        4,
        &testWorkerInit,
        @ptrFromInt(@as(usize, 1234)),
        &testWorkerFini,
        @ptrFromInt(@as(usize, 5678)),
    );
    defer pool.deinit();

    try std.testing.expectEqual(@as(usize, 4), pool.n_jobs);
    try std.testing.expectEqual(std.heap.page_allocator, pool.allocator);
    try std.testing.expect(!pool.started);
}

test "ThreadPool start spawns n_jobs workers" {
    g_test_worker_init_count.store(0, .monotonic);
    g_test_worker_fini_count.store(0, .monotonic);

    var pool = ThreadPool.init(std.heap.page_allocator, 4, &testWorkerInit, null, &testWorkerFini, null);
    defer pool.deinit();

    try pool.start();
    try waitForCounter(&g_test_worker_init_count, 4);

    try std.testing.expect(pool.started);
    try std.testing.expectEqual(@as(usize, 4), g_test_worker_init_count.load(.monotonic));
}

test "ThreadPool addTask executes on worker" {
    g_test_counter.store(0, .monotonic);

    var pool = ThreadPool.init(std.heap.page_allocator, 2, &noOpWorker, null, &noOpWorker, null);
    defer pool.deinit();

    try pool.start();
    pool.addTask(.{ .proc = &testTask, .data = null });
    pool.addTask(.{ .proc = &testTask, .data = null });
    pool.addTask(.{ .proc = &testTask, .data = null });

    try waitForCounter(&g_test_counter, 3);
}

test "ThreadPool join stops all workers" {
    g_test_worker_init_count.store(0, .monotonic);
    g_test_worker_fini_count.store(0, .monotonic);

    var pool = ThreadPool.init(std.heap.page_allocator, 3, &testWorkerInit, null, &testWorkerFini, null);
    try pool.start();
    try waitForCounter(&g_test_worker_init_count, 3);

    pool.join();
    try std.testing.expect(!pool.started);
    try std.testing.expectEqual(@as(usize, 3), g_test_worker_fini_count.load(.monotonic));
    pool.deinit();
}

test "ThreadPool multiple tasks executed" {
    g_test_counter.store(0, .monotonic);
    const task_count: usize = 32;

    var pool = ThreadPool.init(std.heap.page_allocator, 4, &noOpWorker, null, &noOpWorker, null);
    defer pool.deinit();

    try pool.start();
    var i: usize = 0;
    while (i < task_count) : (i += 1) {
        pool.addTask(.{ .proc = &testTask, .data = null });
    }

    try waitForCounter(&g_test_counter, task_count);
}

test "ThreadPool empty pool no-op" {
    g_test_counter.store(0, .monotonic);

    var pool = ThreadPool.init(std.heap.page_allocator, 0, &testWorkerInit, null, &testWorkerFini, null);
    defer pool.deinit();

    try pool.start();
    try std.testing.expect(pool.started);
    pool.addTask(.{ .proc = &testTask, .data = null });
    pool.stopTask(.{ .proc = &testTask, .data = null });
    try std.testing.expectEqual(@as(usize, 0), g_test_counter.load(.monotonic));
}

test "ThreadPool condvar wakes workers" {
    g_test_worker_init_count.store(0, .monotonic);
    g_test_worker_fini_count.store(0, .monotonic);

    var pool = ThreadPool.init(std.heap.page_allocator, 2, &testWorkerInit, null, &testWorkerFini, null);
    try pool.start();
    try waitForCounter(&g_test_worker_init_count, 2);

    pool.join();
    try std.testing.expect(!pool.started);
    try std.testing.expectEqual(@as(usize, 2), g_test_worker_fini_count.load(.monotonic));
    pool.deinit();
}

test "ThreadPool join on not-started" {
    var pool = ThreadPool.init(std.heap.page_allocator, 0, &noOpWorker, null, &noOpWorker, null);
    pool.join(); // should not crash even if not started
    pool.deinit();
}

test "WorkerPool init stores config" {
    var pool = WorkerPool.initWithOptions(
        std.heap.page_allocator,
        4,
        &testWorkerInit,
        @ptrFromInt(@as(usize, 1234)),
        &testWorkerFini,
        @ptrFromInt(@as(usize, 5678)),
        .{ .queue_capacity = 64 },
    );
    defer pool.deinit();

    try std.testing.expectEqual(@as(usize, 4), pool.n_jobs);
    try std.testing.expectEqual(@as(usize, 64), pool.queue_capacity);
    try std.testing.expectEqual(std.heap.page_allocator, pool.allocator);
    try std.testing.expect(!pool.started);
}

test "WorkerPool start spawns n_jobs workers" {
    g_test_worker_init_count.store(0, .monotonic);
    g_test_worker_fini_count.store(0, .monotonic);

    var pool = WorkerPool.init(std.heap.page_allocator, 4, &testWorkerInit, null, &testWorkerFini, null);
    defer pool.deinit();

    try pool.start();
    try waitForCounter(&g_test_worker_init_count, 4);

    try std.testing.expect(pool.started);
    try std.testing.expectEqual(@as(usize, 4), g_test_worker_init_count.load(.monotonic));
}

test "WorkerPool addTask executes on worker" {
    g_test_counter.store(0, .monotonic);

    var pool = WorkerPool.init(std.heap.page_allocator, 2, &noOpWorker, null, &noOpWorker, null);
    defer pool.deinit();

    try pool.start();
    pool.addTask(.{ .proc = &testTask, .data = null });
    pool.addTask(.{ .proc = &testTask, .data = null });
    pool.addTask(.{ .proc = &testTask, .data = null });

    try waitForCounter(&g_test_counter, 3);
}

test "WorkerPool tryAddTask returns false when all queues are full" {
    g_test_worker_init_count.store(0, .monotonic);
    g_test_counter.store(0, .monotonic);
    g_test_block_worker_init.store(true, .release);

    var pool = WorkerPool.initWithOptions(
        std.heap.page_allocator,
        1,
        &blockingWorkerInit,
        null,
        &noOpWorker,
        null,
        .{ .queue_capacity = 1 },
    );
    try pool.start();
    try waitForCounter(&g_test_worker_init_count, 1);

    try std.testing.expect(pool.tryAddTask(.{ .proc = &testTask, .data = null }));
    try std.testing.expect(!pool.tryAddTask(.{ .proc = &testTask, .data = null }));

    g_test_block_worker_init.store(false, .release);
    try waitForCounter(&g_test_counter, 1);
    pool.deinit();
}

test "WorkerPool multiple small tasks executed" {
    g_test_counter.store(0, .monotonic);
    const task_count: usize = 128;

    var pool = WorkerPool.init(std.heap.page_allocator, 4, &noOpWorker, null, &noOpWorker, null);
    defer pool.deinit();

    try pool.start();
    var i: usize = 0;
    while (i < task_count) : (i += 1) {
        try std.testing.expect(pool.tryAddTask(.{ .proc = &testTask, .data = null }));
    }

    try waitForCounter(&g_test_counter, task_count);
}

test "WorkerPool accepts concurrent producers" {
    g_test_counter.store(0, .monotonic);
    const producer_count: usize = 4;
    const tasks_per_producer: usize = 64;

    var pool = WorkerPool.initWithOptions(
        std.heap.page_allocator,
        4,
        &noOpWorker,
        null,
        &noOpWorker,
        null,
        .{ .queue_capacity = 64 },
    );
    defer pool.deinit();
    try pool.start();

    var contexts: [producer_count]ProducerSubmitContext = undefined;
    var producers: [producer_count]std.Thread = undefined;
    for (&producers, 0..) |*producer, index| {
        contexts[index] = .{ .pool = &pool, .count = tasks_per_producer };
        producer.* = try std.Thread.spawn(.{}, producerSubmitLoop, .{&contexts[index]});
    }
    for (producers) |producer| producer.join();

    try waitForCounter(&g_test_counter, producer_count * tasks_per_producer);
}

test "WorkerPool steals tasks from another worker queue" {
    g_test_worker_init_count.store(0, .monotonic);
    g_test_counter.store(0, .monotonic);
    const task_count: usize = 32;

    var pool = WorkerPool.initWithOptions(
        std.heap.page_allocator,
        2,
        &testWorkerInit,
        null,
        &noOpWorker,
        null,
        .{ .queue_capacity = 64 },
    );
    defer pool.deinit();

    try pool.start();
    try waitForCounter(&g_test_worker_init_count, 2);

    const queues = pool.queues.?;
    var i: usize = 0;
    while (i < task_count) : (i += 1) {
        try std.testing.expect(queues[0].tryPush(.{ .proc = &slowTestTask, .data = null }));
    }
    pool.wakeAll();

    try waitForCounter(&g_test_counter, task_count);
    try std.testing.expect(pool.steal_count.load(.monotonic) > 0);
}

test "WorkerPool join stops all workers" {
    g_test_worker_init_count.store(0, .monotonic);
    g_test_worker_fini_count.store(0, .monotonic);

    var pool = WorkerPool.init(std.heap.page_allocator, 3, &testWorkerInit, null, &testWorkerFini, null);
    try pool.start();
    try waitForCounter(&g_test_worker_init_count, 3);

    pool.join();
    try std.testing.expect(!pool.started);
    try std.testing.expectEqual(@as(usize, 3), g_test_worker_fini_count.load(.monotonic));
    pool.deinit();
}

test "WorkerPool empty pool no-op" {
    g_test_counter.store(0, .monotonic);

    var pool = WorkerPool.init(std.heap.page_allocator, 0, &testWorkerInit, null, &testWorkerFini, null);
    defer pool.deinit();

    try pool.start();
    try std.testing.expect(pool.started);
    try std.testing.expect(!pool.tryAddTask(.{ .proc = &testTask, .data = null }));
    pool.addTask(.{ .proc = &testTask, .data = null });
    pool.stopTask(.{ .proc = &testTask, .data = null });
    try std.testing.expectEqual(@as(usize, 0), g_test_counter.load(.monotonic));
}
