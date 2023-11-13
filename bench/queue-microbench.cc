// A microbenchmark for queue insertion throughput, paremeterized on dequeuers,
// queue size, and message size.

#include "absl/log/check.h"
#include "benchmark/benchmark.h"
#include "lib/thread-safe-queue.h"
#include <optional>
#include <thread>
#include <vector>

namespace cancellable_queue {
namespace {

std::optional<ThreadSafeQueue> queue;
std::vector<std::thread> dequeuers;

void QueueAndDequeuersSetup(const benchmark::State &state) {
  int dequeuer_ct = state.range(0); // dequeuers
  queue = ThreadSafeQueue::Make(4 << 20).value();
  for (int i = 0; i < dequeuer_ct; ++i) {
    dequeuers.emplace_back([]() {
      for (auto e = queue->Dequeue(); e.ok(); e = queue->Dequeue()) {
        auto entry = std::move(e).value();
        // When `entry` is destroyed here, the message is released from the
        // queue.
      }
    });
  }
}

void QueueAndDequeuersTearDown(const benchmark::State &state) {
  queue->Cancel();
  for (auto &t : dequeuers) {
    t.join();
  }
  dequeuers.clear();
  queue.reset();
}

static void BM_EnqueueMessage(benchmark::State &state) {
  const auto kMsgSize = state.range(1);
  auto msg = std::make_unique<char[]>(kMsgSize); // range(1) is the msg size
  std::string_view v(msg.get(), kMsgSize);
  for (auto _ : state) {
    CHECK_OK(queue->Enqueue(v));
  }
  state.SetBytesProcessed(state.iterations() * kMsgSize);
};

BENCHMARK(BM_EnqueueMessage)
    ->Setup(QueueAndDequeuersSetup)
    ->Teardown(QueueAndDequeuersTearDown)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->ArgNames({"dequeuers", "msg_size"})
    ->ArgsProduct({
        {1, 2, 4},
        {100, 1 << 10, 4 << 10, 256 << 10, 1 << 20},
    })
    ->ThreadRange(1, 16);

} // namespace
} // namespace cancellable_queue