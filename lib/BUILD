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
