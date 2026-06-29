//! utils — shared utilities (thread pools, spinlock, SFL allocator).
//!
//! wasm32-freestanding: `wasm_stub.zig` (no std.Thread).
//! Native: `thread_native.zig`.

const builtin = @import("builtin");

const is_wasm_freestanding = builtin.cpu.arch.isWasm() and builtin.os.tag == .freestanding;

pub const SpinLock = @import("spin_lock.zig").SpinLock;
pub const SFL = @import("sfl_allocator.zig").SFL;

const thread_impl = if (is_wasm_freestanding)
    @import("wasm_stub.zig")
else
    @import("thread_native.zig");

pub const WaitGroup = thread_impl.WaitGroup;
pub const Task = thread_impl.Task;
pub const ThreadPool = thread_impl.ThreadPool;
pub const WorkerPool = thread_impl.WorkerPool;
pub const WorkerPoolOptions = thread_impl.WorkerPoolOptions;
pub const DefaultWorkerPoolQueueCapacity = thread_impl.DefaultWorkerPoolQueueCapacity;
pub const noOpWorker = thread_impl.noOpWorker;
