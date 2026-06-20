//! utils.sfl_allocator — Segregated Free List 할당자.
//!
//! - 단일 스레드, 락 없음
//! - 핫 패스에 매직/디버그 검증 없음
//! - Small: class 0..NumClasses-1, free-list + 세그먼트 bump
//! - Large: class == LargeClass, 별도 가상 메모리 매핑
//!
//! Odin `sfl_allocator.odin` → Zig 0.16.0 포팅.
//! `utils.SFL`로 re-export 된다.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

const KiB = 1024;
const MiB = 1024 * KiB;

const alignment: usize = 16;
const minSize: usize = 16;
const tinyMaxSize: usize = 256;
const smallMaxSize: usize = 4096;
const maxSlabSize: usize = 64 * KiB;
const segmentSize: usize = 8 * MiB;

const tinyClassStep: usize = 16;
const smallClassStep: usize = 256;
const mediumClassStep: usize = 4 * KiB;

const TinyClassCount: usize = tinyMaxSize / tinyClassStep;
const SmallClassCount: usize = 16;
const MediumClassCount: usize = 16;
const NumClasses: usize = TinyClassCount + SmallClassCount + MediumClassCount;
const LargeClass: u16 = @intCast(NumClasses);

const AllocationHeader = struct {
    class: u16,
    _pad: u16 = 0,
    rawBase: [*]u8 = undefined,
    reservedSize: usize = 0,
};

const headerSize: usize = blk: {
    const size = @sizeOf(AllocationHeader);
    break :blk ((size + alignment - 1) / alignment) * alignment;
};

const Node = struct {
    next: ?*Node,
};

const Segment = struct {
    next: ?*Segment,
    size: usize,
    cursor: usize,
    payloadStart: usize,
    payloadLimit: usize,
};

pub const SFL = struct {
    freeLists: [NumClasses]?*Node = [_]?*Node{null} ** NumClasses,
    segHead: ?*Segment = null,
    segSize: usize = 0,
};

// ============================================================
// Helpers
// ============================================================

inline fn alignUp(x: usize, algn: usize) usize {
    return std.mem.alignForward(usize, x, algn);
}

inline fn sizeToClass(size: usize) usize {
    if (size < tinyMaxSize) {
        return ((size + tinyClassStep - 1) / tinyClassStep) - 1;
    }
    if (size < smallMaxSize) {
        return TinyClassCount + ((size - tinyMaxSize + smallClassStep - 1) / smallClassStep);
    }
    return TinyClassCount + SmallClassCount + ((size - smallMaxSize + mediumClassStep - 1) / mediumClassStep);
}

inline fn classToSize(class: usize) usize {
    if (class < TinyClassCount) {
        return (class + 1) * tinyClassStep;
    }
    if (class < TinyClassCount + SmallClassCount) {
        return tinyMaxSize + (class - TinyClassCount) * smallClassStep;
    }
    return smallMaxSize + (class - TinyClassCount - SmallClassCount) * mediumClassStep;
}

inline fn ptrHeader(ptr: [*]u8) *AllocationHeader {
    return @ptrFromInt(@intFromPtr(ptr) - headerSize);
}

inline fn slotBase(ptr: [*]u8) [*]u8 {
    return @ptrFromInt(@intFromPtr(ptr) - headerSize);
}

inline fn segmentInitPayloadBounds(seg: *Segment) void {
    seg.payloadStart = std.mem.alignForward(usize, @intFromPtr(seg) + @sizeOf(Segment), alignment);
    seg.payloadLimit = @intFromPtr(seg) + seg.size;
}

fn osAlloc(size: usize) ?[*]u8 {
    const pageSize = std.heap.pageSize();
    const alignedSize = alignUp(size, pageSize);
    return std.heap.page_allocator.rawAlloc(alignedSize, .@"1", @returnAddress());
}

inline fn osFree(raw: [*]u8, size: usize) void {
    const pageSize = std.heap.pageSize();
    const alignedSize = alignUp(size, pageSize);
    std.heap.page_allocator.rawFree(raw[0..alignedSize], .@"1", @returnAddress());
}

inline fn segBumpAlloc(seg: *Segment, blockSize: usize) ?[*]u8 {
    const cursor = alignUp(seg.cursor, alignment);
    const end = cursor + blockSize;
    if (end > seg.payloadLimit - seg.payloadStart) {
        return null;
    }
    const base: [*]u8 = @ptrFromInt(seg.payloadStart + cursor);
    seg.cursor = end;
    return base;
}

fn newSegment(g: *SFL, minBlock: usize) ?*Segment {
    var segSz = g.segSize;
    if (segSz == 0) {
        segSz = segmentSize;
    }

    const needed = minBlock + @sizeOf(Segment) + alignment;
    while (segSz < needed) {
        segSz *= 2;
    }
    segSz = alignUp(segSz, std.heap.pageSize());

    const raw = osAlloc(segSz) orelse return null;

    const seg: *Segment = @ptrCast(@alignCast(raw));
    seg.next = g.segHead;
    seg.size = segSz;
    seg.cursor = 0;
    segmentInitPayloadBounds(seg);
    g.segHead = seg;
    return seg;
}

fn freeSegment(seg: *Segment) void {
    osFree(@ptrCast(seg), seg.size);
}

inline fn writeSmallHeader(ptr: [*]u8, class: u16) void {
    ptrHeader(ptr).class = class;
}

inline fn writeLargeHeader(ptr: [*]u8, raw: [*]u8, reserved: usize) void {
    const h = ptrHeader(ptr);
    h.class = LargeClass;
    h.rawBase = raw;
    h.reservedSize = reserved;
}

fn allocSmall(g: *SFL, size: usize) ?[*]u8 {
    const class = sizeToClass(@max(size, minSize));
    const blockSize = headerSize + classToSize(class);

    const head = g.freeLists[class];
    const basePtr: [*]u8 = if (head) |h| blk: {
        g.freeLists[class] = h.next;
        break :blk @ptrCast(h);
    } else blk: {
        var b = segBumpAlloc(g.segHead orelse return null, blockSize);
        if (b == null) {
            if (newSegment(g, blockSize) == null) {
                return null;
            }
            b = segBumpAlloc(g.segHead.?, blockSize);
        }
        break :blk b orelse return null;
    };
    const ptr: [*]u8 = @ptrFromInt(@intFromPtr(basePtr) + headerSize);
    writeSmallHeader(ptr, @intCast(class));
    return ptr;
}

fn allocLarge(size: usize, algn: usize) ?[*]u8 {
    const requestedAlignment = @max(algn, alignment);
    const total = headerSize + size + requestedAlignment - 1;
    const reservedSize = alignUp(total, std.heap.pageSize());
    const raw = osAlloc(reservedSize) orelse return null;

    const ptr: [*]u8 = @ptrFromInt(std.mem.alignForward(usize, @intFromPtr(raw) + headerSize, requestedAlignment));
    writeLargeHeader(ptr, raw, reservedSize);
    return ptr;
}

// ============================================================
// Public API
// ============================================================

pub fn init(g: *SFL, segSz: usize) bool {
    g.* = .{};
    g.segSize = alignUp(@max(segSz, 4096), std.heap.pageSize());
    return newSegment(g, 0) != null;
}

pub fn destroy(g: *SFL) void {
    var seg = g.segHead;
    while (seg != null) {
        const next = seg.?.next;
        freeSegment(seg.?);
        seg = next;
    }
    g.* = .{};
}

pub inline fn alloc(g: *SFL, size: usize, algn: usize) ?[*]u8 {
    if (algn <= alignment and size <= maxSlabSize) {
        return allocSmall(g, size);
    }
    return allocLarge(size, algn);
}

pub inline fn free(g: *SFL, ptr: [*]u8) void {
    const h = ptrHeader(ptr);
    if (h.class >= LargeClass) {
        osFree(h.rawBase, h.reservedSize);
        return;
    }
    const class: usize = h.class;
    const node: *Node = @ptrCast(@alignCast(slotBase(ptr)));
    node.next = g.freeLists[class];
    g.freeLists[class] = node;
}

pub fn resize(
    g: *SFL,
    ptr: ?[*]u8,
    oldSize: usize,
    newSize: usize,
    algn: usize,
) ?[*]u8 {
    if (ptr == null) {
        return alloc(g, newSize, algn);
    }
    if (newSize == 0) {
        free(g, ptr.?);
        return null;
    }

    const h = ptrHeader(ptr.?);
    if (h.class >= LargeClass) {
        if (newSize <= h.reservedSize - headerSize) {
            return ptr;
        }
    } else if (algn <= alignment) {
        if (newSize <= classToSize(@intCast(h.class))) {
            return ptr;
        }
    }

    const newPtr = alloc(g, newSize, algn) orelse return null;

    const copySize = @min(oldSize, newSize);
    if (newPtr != ptr and copySize > 0) {
        @memcpy(newPtr[0..copySize], ptr.?[0..copySize]);
    }
    free(g, ptr.?);
    return newPtr;
}

// ============================================================
// Zig Allocator Interface
// ============================================================

fn vtableAlloc(ctx: *anyopaque, len: usize, alignment_vt: Alignment, ret_addr: usize) ?[*]u8 {
    _ = ret_addr;
    const g: *SFL = @ptrCast(@alignCast(ctx));
    return alloc(g, len, alignment_vt.toByteUnits());
}

fn vtableResize(
    ctx: *anyopaque,
    memory: []u8,
    alignment_vt: Alignment,
    new_len: usize,
    ret_addr: usize,
) bool {
    _ = ret_addr;
    const g: *SFL = @ptrCast(@alignCast(ctx));
    if (new_len == 0) {
        free(g, memory.ptr);
        return true;
    }
    if (memory.len == 0) {
        return false;
    }

    const h = ptrHeader(memory.ptr);
    if (h.class >= LargeClass) {
        return new_len <= h.reservedSize - headerSize;
    } else if (alignment_vt.toByteUnits() <= alignment) {
        return new_len <= classToSize(@intCast(h.class));
    }
    return false;
}

fn vtableRemap(
    ctx: *anyopaque,
    memory: []u8,
    alignment_vt: Alignment,
    new_len: usize,
    ret_addr: usize,
) ?[*]u8 {
    const g: *SFL = @ptrCast(@alignCast(ctx));
    if (new_len == 0) {
        free(g, memory.ptr);
        return null;
    }
    if (memory.len == 0) {
        return null;
    }

    // 같은 포인터에 resize 시도
    if (vtableResize(ctx, memory, alignment_vt, new_len, ret_addr)) {
        return memory.ptr;
    }

    // resize 불가 → caller가 alloc+copy+free 하도록 null 반환
    return null;
}

fn vtableFree(
    ctx: *anyopaque,
    memory: []u8,
    alignment_vt: Alignment,
    ret_addr: usize,
) void {
    _ = alignment_vt;
    _ = ret_addr;
    const g: *SFL = @ptrCast(@alignCast(ctx));
    free(g, memory.ptr);
}

const vtable: Allocator.VTable = .{
    .alloc = vtableAlloc,
    .resize = vtableResize,
    .remap = vtableRemap,
    .free = vtableFree,
};

pub fn allocator(g: *SFL) Allocator {
    return .{ .ptr = g, .vtable = &vtable };
}

// ============================================================
// Tests
// ============================================================

test "sfl class mapping" {
    try std.testing.expectEqual(@as(usize, 48), NumClasses);
    try std.testing.expectEqual(@as(usize, 0), sizeToClass(1));
    try std.testing.expectEqual(@as(usize, 0), sizeToClass(16));
    try std.testing.expectEqual(@as(usize, 1), sizeToClass(17));
    try std.testing.expectEqual(@as(usize, 15), sizeToClass(255));
    try std.testing.expectEqual(@as(usize, 16), sizeToClass(256));
    try std.testing.expectEqual(@as(usize, 17), sizeToClass(257));
    try std.testing.expectEqual(@as(usize, 31), sizeToClass(4095));
    try std.testing.expectEqual(@as(usize, 32), sizeToClass(4096));
    try std.testing.expectEqual(@as(usize, 33), sizeToClass(4097));
    try std.testing.expectEqual(@as(usize, 47), sizeToClass(64 * KiB));

    try std.testing.expectEqual(@as(usize, 16), classToSize(0));
    try std.testing.expectEqual(@as(usize, 256), classToSize(15));
    try std.testing.expectEqual(@as(usize, 256), classToSize(16));
    try std.testing.expectEqual(@as(usize, 512), classToSize(17));
    try std.testing.expectEqual(@as(usize, 4096), classToSize(31));
    try std.testing.expectEqual(@as(usize, 4096), classToSize(32));
    try std.testing.expectEqual(@as(usize, 8192), classToSize(33));
    try std.testing.expectEqual(@as(usize, 64 * KiB), classToSize(47));
}

test "sfl small alloc free reuse" {
    var g: SFL = .{};
    try std.testing.expect(init(&g, 4096));
    defer destroy(&g);

    const a = alloc(&g, 32, alignment).?;
    const b = alloc(&g, 32, alignment).?;
    try std.testing.expect(@intFromPtr(a) % alignment == 0);
    free(&g, a);
    const c = alloc(&g, 32, alignment).?;
    try std.testing.expectEqual(a, c);
    free(&g, b);
    free(&g, c);
}

test "sfl large alloc individual free" {
    var g: SFL = .{};
    try std.testing.expect(init(&g, 4096));
    defer destroy(&g);

    const big = alloc(&g, 64 * KiB + 1, alignment).?;
    @memset(big[0 .. 64 * KiB + 1], 0xab);
    free(&g, big);

    const bigger = alloc(&g, 2 * MiB, alignment).?;
    @memset(bigger[0 .. 2 * MiB], 0xcd);
    free(&g, bigger);
}

test "sfl overaligned uses large path" {
    var g: SFL = .{};
    try std.testing.expect(init(&g, 4096));
    defer destroy(&g);

    const p = alloc(&g, 64, 256).?;
    try std.testing.expect(@intFromPtr(p) % 256 == 0);
    free(&g, p);
}

test "sfl allocator interface" {
    var g: SFL = .{};
    try std.testing.expect(init(&g, 4096));
    defer destroy(&g);

    const al = allocator(&g);

    const data = try al.alloc(u8, 128);
    defer al.free(data);
    try std.testing.expectEqual(@as(usize, 128), data.len);

    for (data, 0..) |*b, i| {
        b.* = @intCast(i);
    }

    const newData = try al.realloc(data, 4096);
    try std.testing.expectEqual(@as(usize, 4096), newData.len);
    for (newData[0..128], 0..) |b, i| {
        try std.testing.expectEqual(@as(u8, @intCast(i)), b);
    }
}

test "sfl dynamic array append" {
    const SceneEntry = struct {
        vtable: *anyopaque,
        imp: *anyopaque,
    };

    var g: SFL = .{};
    try std.testing.expect(init(&g, 4096));
    defer destroy(&g);

    const al = allocator(&g);

    var scene: std.ArrayList(SceneEntry) = .empty;
    defer scene.deinit(al);

    for (0..64) |_| {
        try scene.append(al, .{ .vtable = undefined, .imp = undefined });
    }
    try std.testing.expectEqual(@as(usize, 64), scene.items.len);
}

test "sfl stress" {
    const Slot = struct {
        ptr: ?[*]u8 = null,
        size: usize = 0,
        algn: usize = 0,
    };

    var g: SFL = .{};
    try std.testing.expect(init(&g, 4096));
    defer destroy(&g);

    var slots: [256]Slot = [_]Slot{.{}} ** 256;
    var state: usize = 0x12345678;

    const nextRand = struct {
        fn f(statePtr: *usize) usize {
            statePtr.* = statePtr.* *% 1664525 +% 1013904223;
            return statePtr.*;
        }
    }.f;

    for (0..20000) |_| {
        const idx = @as(usize, nextRand(&state) % slots.len);
        if (slots[idx].ptr != null and (nextRand(&state) & 3) != 0) {
            free(&g, slots[idx].ptr.?);
            slots[idx] = .{};
            continue;
        }

        const size = (nextRand(&state) % (96 * KiB)) + 1;
        var algn = alignment;
        if ((nextRand(&state) & 15) == 0) {
            algn = alignment << @intCast(nextRand(&state) % 5);
        }

        if (slots[idx].ptr) |_| {
            const newPtr = resize(&g, slots[idx].ptr, slots[idx].size, size, slots[idx].algn);
            try std.testing.expect(newPtr != null);
            try std.testing.expect(@intFromPtr(newPtr.?) % slots[idx].algn == 0);
            slots[idx] = .{ .ptr = newPtr, .size = size, .algn = slots[idx].algn };
        } else {
            const ptr = alloc(&g, size, algn);
            try std.testing.expect(ptr != null);
            try std.testing.expect(@intFromPtr(ptr.?) % algn == 0);
            @memset(ptr.?[0..size], @as(u8, @intCast(idx)));
            slots[idx] = .{ .ptr = ptr, .size = size, .algn = algn };
        }
    }

    for (&slots) |*slot| {
        if (slot.ptr) |p| {
            free(&g, p);
            slot.* = .{};
        }
    }
}