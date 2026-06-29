//! wasm32-freestanding thread pool stubs — no std.Thread, no Io.Threaded.
//!
//! Tasks run synchronously on the caller thread. n_jobs is always 0 so engine2
//! e2 VM submitTask takes the direct synchronous path.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Task = struct {
    proc: *const fn (data: ?*anyopaque) void,
    data: ?*anyopaque = null,
};

/// Single-threaded ThreadPool. Never spawns workers.
pub const ThreadPool = struct {
    allocator: Allocator,
    n_jobs: usize = 0,
    worker_init: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_init_data: ?*anyopaque = null,
    worker_fini: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_fini_data: ?*anyopaque = null,
    started: bool = false,

    pub fn init(
        allocator: Allocator,
        n_jobs: usize,
        worker_init: *const fn (data: ?*anyopaque) void,
        worker_init_data: ?*anyopaque,
        worker_fini: *const fn (data: ?*anyopaque) void,
        worker_fini_data: ?*anyopaque,
    ) ThreadPool {
        _ = n_jobs;
        return .{
            .allocator = allocator,
            .n_jobs = 0,
            .worker_init = worker_init,
            .worker_init_data = worker_init_data,
            .worker_fini = worker_fini,
            .worker_fini_data = worker_fini_data,
        };
    }

    pub fn deinit(self: *ThreadPool) void {
        _ = self;
    }

    pub fn start(self: *ThreadPool) !void {
        self.started = true;
    }

    pub fn addTask(self: *ThreadPool, task: Task) void {
        _ = self;
        task.proc(task.data);
    }

    pub fn join(self: *ThreadPool) void {
        self.started = false;
    }

    pub fn stopTask(self: *ThreadPool, task: Task) void {
        self.addTask(task);
    }
};

pub const WorkerPool = struct {
    allocator: Allocator,
    n_jobs: usize = 0,
    queue_capacity: usize = 256,
    worker_init: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_init_data: ?*anyopaque = null,
    worker_fini: *const fn (data: ?*anyopaque) void = &noOpWorker,
    worker_fini_data: ?*anyopaque = null,
    started: bool = false,

    pub fn init(
        allocator: Allocator,
        n_jobs: usize,
        worker_init: *const fn (data: ?*anyopaque) void,
        worker_init_data: ?*anyopaque,
        worker_fini: *const fn (data: ?*anyopaque) void,
        worker_fini_data: ?*anyopaque,
    ) WorkerPool {
        _ = n_jobs;
        return .{
            .allocator = allocator,
            .n_jobs = 0,
            .worker_init = worker_init,
            .worker_init_data = worker_init_data,
            .worker_fini = worker_fini,
            .worker_fini_data = worker_fini_data,
        };
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
        _ = n_jobs;
        return .{
            .allocator = allocator,
            .n_jobs = 0,
            .queue_capacity = options.queue_capacity,
            .worker_init = worker_init,
            .worker_init_data = worker_init_data,
            .worker_fini = worker_fini,
            .worker_fini_data = worker_fini_data,
        };
    }

    pub fn deinit(self: *WorkerPool) void {
        _ = self;
    }

    pub fn start(self: *WorkerPool) !void {
        self.started = true;
    }

    pub fn addTask(self: *WorkerPool, task: Task) void {
        _ = self;
        task.proc(task.data);
    }

    pub fn tryAddTask(self: *WorkerPool, task: Task) bool {
        _ = self;
        task.proc(task.data);
        return true;
    }

    pub fn join(self: *WorkerPool) void {
        self.started = false;
    }

    pub fn stopTask(self: *WorkerPool, task: Task) void {
        self.addTask(task);
    }
};

pub const WorkerPoolOptions = struct {
    queue_capacity: usize = 256,
};

pub const DefaultWorkerPoolQueueCapacity: usize = 256;

/// No-op WaitGroup — wasm is single-threaded.
pub const WaitGroup = struct {
    pub fn add(_: *WaitGroup, _: usize) void {}
    pub fn done(_: *WaitGroup) void {}
    pub fn wait(_: *WaitGroup) void {}
};

pub fn noOpWorker(_: ?*anyopaque) void {}
