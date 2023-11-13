cc_binary(
    name = "queue-microbench",
    srcs = ["queue-microbench.cc"],
    deps = [
        "//lib:thread-safe-queue",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/synchronization",
        "@google_benchmark//:benchmark_main",
    ],
)
