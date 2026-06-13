const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const thread_pool_mod = b.addModule("thread_pool", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const tests = b.addTest(.{
        .root_module = thread_pool_mod,
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "단위 테스트");
    test_step.dependOn(&run_tests.step);

    const check_step = b.step("check", "전체 컴파일 체크");
    check_step.dependOn(&run_tests.step);

}
