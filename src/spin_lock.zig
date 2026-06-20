//! utils.spin_lock — 공용 SpinLock.
//!
//! 여러 모듈에서 중복 정의되던 SpinLock을 하나로 통합.
//! atomic swap + spinLoopHint 패턴으로 구현.
//!
//! 사용법:
//!   const utils = @import("utils");
//!   const SpinLock = utils.SpinLock;
//!   var lock: SpinLock = .{};
//!   lock.lock();
//!   defer lock.unlock();

const std = @import("std");

pub const SpinLock = struct {
    locked: std.atomic.Value(bool) = .init(false),

    pub fn lock(self: *SpinLock) void {
        while (self.locked.swap(true, .acquire)) {
            while (self.locked.load(.monotonic)) {
                std.atomic.spinLoopHint();
            }
        }
    }

    pub fn unlock(self: *SpinLock) void {
        self.locked.store(false, .release);
    }

    pub fn tryLock(self: *SpinLock) bool {
        return !self.locked.swap(true, .acquire);
    }
};

test "SpinLock basic lock/unlock" {
    var lock: SpinLock = .{};
    lock.lock();
    lock.unlock();
}

test "SpinLock tryLock" {
    var lock: SpinLock = .{};
    try std.testing.expect(lock.tryLock());
    try std.testing.expect(!lock.tryLock());
    lock.unlock();
    try std.testing.expect(lock.tryLock());
    lock.unlock();
}
