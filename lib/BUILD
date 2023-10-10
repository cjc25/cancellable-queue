cc_library(
    name = "ring",
    srcs = ["ring.cc"],
    hdrs = ["ring.h"],
    visibility = ["//visibility:private"],
    deps = ["@abseil-cpp//absl/status:statusor"],
)

cc_test(
    name = "ring-test",
    srcs = ["ring-test.cc"],
    deps = [
        ":ring",
        "@abseil-cpp//absl/cleanup",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "thread-safe-queue",
    srcs = ["thread-safe-queue.cc"],
    hdrs = ["thread-safe-queue.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":ring",
        "@abseil-cpp//absl/log:absl_check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/synchronization",
    ],
)

cc_test(
    name = "queue-test",
    srcs = ["queue-test.cc"],
    deps = [
        ":thread-safe-queue",
        "@abseil-cpp//absl/synchronization",
        "@googletest//:gtest_main",
    ],
)
