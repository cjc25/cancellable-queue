cc_binary(
    name = "queue-bench",
    srcs = ["queue-bench.cc"],
    deps = [
        "//lib:thread-safe-queue",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/flags:parse",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/log:flags",
        "@abseil-cpp//absl/log:initialize",
        "@abseil-cpp//absl/random",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
    ],
)

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