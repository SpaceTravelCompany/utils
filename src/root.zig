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

pub fn noOpWorker(_: ?*anyopaque) void {}

var g_test_worker_init_count: std.atomic.Value(usize) = .init(0);
var g_test_worker_fini_count: std.atomic.Value(usize) = .init(0);
var g_test_counter: std.atomic.Value(usize) = .init(0);

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
