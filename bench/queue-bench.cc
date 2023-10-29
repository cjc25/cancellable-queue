#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "lib/thread-safe-queue.h"
#include <iostream>
#include <map>
#include <sys/resource.h>
#include <sys/time.h>
#include <thread>
#include <utility>
#include <variant>

ABSL_FLAG(int64_t, measurements, 10'000, "number of measurements to take.");
ABSL_FLAG(int, queue_pages, 1024,
          "number of memory pages to allocate for the queue.");
ABSL_FLAG(int, dequeuers, 1, "number of dequeue threads for the benchmark.");
ABSL_FLAG(int, enqueuers, 1, "number of enqueue threads for the benchmark.");
ABSL_FLAG(absl::Duration, enqueuer_mean_interarrival_time,
          absl::Microseconds(500),
          "average time between insertions on each enqueuer thread. Note that "
          "if the queue is backed up, requests may not be scheduled on time.");

namespace {

int64_t PageSize() {
  static const int64_t kPageSize = sysconf(_SC_PAGE_SIZE);
  return kPageSize;
}

} // namespace

namespace cancellable_queue {
namespace {

class BytesPerSecond {
public:
  explicit BytesPerSecond(double b) : bps_(b) {}

private:
  template <typename Sink>
  friend void AbslStringify(Sink &s, const BytesPerSecond &b) {
    if (b.bps_ < 1024) {
      absl::Format(&s, "%.2fB/s", b.bps_);
    } else if (b.bps_ < 1024 * 1024) {
      absl::Format(&s, "%.2fKiB/s", (static_cast<double>(b.bps_) / 1024));
    } else if (b.bps_ < 1024 * 1024 * 1024) {
      absl::Format(&s, "%.2fMiB/s",
                   (static_cast<double>(b.bps_) / (1024 * 1024)));
    } else {

      absl::Format(&s, "%.2fGiB/s",
                   (static_cast<double>(b.bps_) / (1024 * 1024 * 1024)));
    }
  }
  double bps_;
};

class Bytes {
public:
  Bytes(int64_t b) : bytes_(b) {}

  Bytes &operator+=(Bytes other) {
    bytes_ += other.bytes_;
    return *this;
  }

private:
  friend BytesPerSecond operator/(Bytes, const absl::Duration &);
  template <typename Sink> friend void AbslStringify(Sink &s, Bytes b) {
    if (b.bytes_ < 1024) {
      absl::Format(&s, "%dB", b.bytes_);
    } else if (b.bytes_ < 1024 * 1024) {
      absl::Format(&s, "%.2fKiB", (static_cast<double>(b.bytes_) / 1024));
    } else if (b.bytes_ < 1024 * 1024 * 1024) {
      absl::Format(&s, "%.2fMiB",
                   (static_cast<double>(b.bytes_) / (1024 * 1024)));
    } else {

      absl::Format(&s, "%.2fGiB",
                   (static_cast<double>(b.bytes_) / (1024 * 1024 * 1024)));
    }
  }

  int64_t bytes_ = 0;
};

BytesPerSecond operator/(Bytes b, const absl::Duration &d) {
  double s = absl::ToDoubleSeconds(d);
  return BytesPerSecond(b.bytes_ / s);
}

struct Timing {
  int enqueuer;

  absl::Time start;
  absl::Time dequeued;
  absl::Time released;

  // N.B. enqueued may not strictly precede dequeued if the dequeuer processes
  // the record before Enqueue() returns to the enqueuer function. The other
  // times are monotonically increasing.
  absl::Time enqueued;

  int64_t msg_size;
  int completion_index;

  absl::Duration dequeue_time() const { return dequeued - start; }
  absl::Duration total_time() const { return released - start; }

  absl::Duration enqueue_time() const { return enqueued - start; }
};

template <size_t MessageLen> struct Message {
  constexpr Message() = default;

  Timing *timing = nullptr;
  char content[MessageLen - sizeof(decltype(timing))] =
      "some content, padded with 0s";
};

using MessageTypes =
    std::variant<Message<100>, Message<1 << 10>, Message<4 << 10>,
                 Message<256 << 10>, Message<1 << 20>>;
constexpr size_t kMessageTypeCount = std::variant_size_v<MessageTypes>;

constexpr std::array<MessageTypes, kMessageTypeCount> kMsgs = []() {
  auto init = []<size_t... Is>(std::index_sequence<Is...>) {
    return std::array<MessageTypes, kMessageTypeCount>{
        std::variant_alternative_t<Is, MessageTypes>()...};
  };
  auto result = init(std::make_index_sequence<kMessageTypeCount>());
  std::sort(result.begin(), result.end(), [](const auto &l, const auto &r) {
    return sizeof(decltype(l)) < sizeof(decltype(r));
  });
  return result;
}();

constexpr size_t kMinMsgSize = []() {
  auto min = []<size_t... Is>(std::index_sequence<Is...>) {
    return std::min({sizeof(std::variant_alternative_t<Is, MessageTypes>)...});
  };
  return min(std::make_index_sequence<kMessageTypeCount>());
}();

constexpr size_t kMaxMsgSize = []() {
  auto max = []<size_t... Is>(std::index_sequence<Is...>) {
    return std::max({sizeof(std::variant_alternative_t<Is, MessageTypes>)...});
  };
  return max(std::make_index_sequence<kMessageTypeCount>());
}();

} // namespace
} // namespace cancellable_queue

ABSL_FLAG(size_t, min_msg_size, cancellable_queue::kMinMsgSize,
          "minimum message size, in bytes");
ABSL_FLAG(size_t, limit_msg_size, cancellable_queue::kMaxMsgSize + 1,
          "upper limit on the message size, in bytes");

namespace cancellable_queue {
namespace {

class Benchmark {
public:
  struct Options {
    static Options Make(int64_t measurements, int queue_pages, int dequeuers,
                        int enqueuers, size_t min_msg_size,
                        size_t limit_msg_size,
                        absl::Duration enqueuer_mean_interarrival_time) {
      Options o;
      o.measurements = measurements;
      o.queue_capacity = PageSize() * queue_pages;
      o.dequeuers = dequeuers;
      o.enqueuers = enqueuers;

      CHECK_LT(min_msg_size, limit_msg_size)
          << "must support at least one message size";
      o.msgs_start = std::lower_bound(
          kMsgs.begin(), kMsgs.end(), min_msg_size,
          [](const auto &e, size_t s) {
            return std::visit(
                [s](const auto &m) { return sizeof(decltype(m)) < s; }, e);
          });
      o.msgs_limit = std::upper_bound(
          kMsgs.begin(), kMsgs.end(), limit_msg_size,
          [](size_t s, const auto &e) {
            return std::visit(
                [s](const auto &m) { return s < sizeof(decltype(m)); }, e);
          });
      CHECK_LT(o.msgs_start, o.msgs_limit)
          << "no messages with size in range [" << min_msg_size << ","
          << limit_msg_size << ")";

      o.enqueuer_mean_interarrival_time = enqueuer_mean_interarrival_time;

      return o;
    }

    int64_t measurements;
    int64_t queue_capacity;
    int dequeuers;
    int enqueuers;

    // Limits on the messages chosen by the enqueuers
    decltype(kMsgs)::const_iterator msgs_start;
    decltype(kMsgs)::const_iterator msgs_limit;

    // The mean interarrival time for each enqueuer.
    absl::Duration enqueuer_mean_interarrival_time;

  private:
    Options() = default;
  };

  explicit Benchmark(const Options &o)
      : lambda_(1.0 / absl::ToDoubleSeconds(o.enqueuer_mean_interarrival_time)),
        msgs_start_(o.msgs_start), msgs_limit_(o.msgs_limit),
        queue_(*ThreadSafeQueue::Make(o.queue_capacity)), next_enqueuer_(0),
        next_op_(0), timings_(o.measurements), completions_(o.measurements),
        dequeuers_(o.dequeuers), enqueuers_(o.enqueuers),
        run_wall_time_(absl::ZeroDuration()),
        run_cpu_time_(absl::ZeroDuration()) {
    LOG(INFO) << "Target mean interarrival time: "
              << o.enqueuer_mean_interarrival_time;
    LOG(INFO) << "Queue capacity: " << Bytes(o.queue_capacity);
    LOG(INFO) << "Measurements: " << o.measurements;
  }

  void Run() {
    absl::Time start = absl::Now();
    struct rusage initial_rusage;
    if (getrusage(RUSAGE_SELF, &initial_rusage) == -1) {
      PLOG(FATAL) << "couldn't get initial rusage";
    }

    for (auto &d : dequeuers_) {
      d = std::thread(&Benchmark::Dequeuer, this);
    }
    for (auto &e : enqueuers_) {
      e = std::thread(&Benchmark::Enqueuer, this);
    }

    for (auto &c : completions_) {
      c.WaitForNotification();
    }

    queue_.Cancel();

    for (auto &d : dequeuers_) {
      d.join();
    }
    for (auto &e : enqueuers_) {
      e.join();
    }

    struct rusage final_rusage;
    if (getrusage(RUSAGE_SELF, &final_rusage) == -1) {
      PLOG(FATAL) << "couldn't get final rusage";
    }
    run_wall_time_ = absl::Now() - start;
    run_cpu_time_ = absl::Microseconds(total_rusage_cpu_usec(final_rusage) -
                                       total_rusage_cpu_usec(initial_rusage));
  }

  void Report() {
    LOG(INFO) << "Throughput metrics";
    LOG(INFO) << "Total CPU:             " << run_cpu_time_;
    LOG(INFO) << "  CPU per op:          " << run_cpu_time_ / timings_.size();
    LOG(INFO) << "Total wall time:       " << run_wall_time_;
    LOG(INFO) << "  Wall time per op:    " << run_wall_time_ / timings_.size();

    Bytes total_bytes = 0;
    absl::Duration total_enqueue_time;
    absl::Duration total_dequeue_time;
    absl::Duration total_total_time;
    std::map<int, std::tuple<absl::Time, absl::Duration, int>>
        per_thread_interarrival_time;
    std::sort(timings_.begin(), timings_.end(),
              [](const auto &l, const auto &r) { return l.start < r.start; });
    for (const auto &t : timings_) {
      total_bytes += t.msg_size;
      total_enqueue_time += t.enqueue_time();
      total_dequeue_time += t.dequeue_time();
      total_total_time += t.total_time();

      auto [it, inserted] = per_thread_interarrival_time.try_emplace(
          t.enqueuer, std::make_tuple(t.start, absl::ZeroDuration(), 0));
      if (!inserted) {
        auto &[start, dur, ct] = it->second;
        dur += t.start - start;
        ++ct;
        start = t.start;
      }
    }
    std::map<int, absl::Duration> mean_interarrival_time;
    std::transform(per_thread_interarrival_time.begin(),
                   per_thread_interarrival_time.end(),
                   std::insert_iterator(mean_interarrival_time,
                                        mean_interarrival_time.end()),
                   [](const auto &t) {
                     return std::make_pair(t.first, std::get<1>(t.second) /
                                                        std::get<2>(t.second));
                   });

    LOG(INFO) << "Total bytes:           " << total_bytes;
    LOG(INFO) << "  Bps (wall time):     " << total_bytes / run_wall_time_;
    LOG(INFO) << "  Bps (CPU time):      " << total_bytes / run_cpu_time_;
    LOG(INFO) << "Mean enqueue time:     "
              << total_enqueue_time / timings_.size();
    LOG(INFO) << "Mean dequeue time:     "
              << total_dequeue_time / timings_.size();
    LOG(INFO) << "Mean time to release:  "
              << total_total_time / timings_.size();

    LOG(INFO) << "Mean interarrival time";
    for (const auto &mit : mean_interarrival_time) {
      LOG(INFO) << absl::StrFormat("Thread % 4d:           ", mit.first)
                << mit.second;
    }

    LOG(INFO) << "Latency metrics";

    std::sort(timings_.begin(), timings_.end(),
              [](const auto &l, const auto &r) {
                return l.enqueue_time() < r.enqueue_time();
              });
    LOG(INFO) << "Enqueue p50:           "
              << timings_[timings_.size() * 0.5].enqueue_time();
    LOG(INFO) << "Enqueue p90:           "
              << timings_[timings_.size() * 0.9].enqueue_time();
    LOG(INFO) << "Enqueue p95:           "
              << timings_[timings_.size() * 0.95].enqueue_time();
    LOG(INFO) << "Enqueue p99:           "
              << timings_[timings_.size() * 0.99].enqueue_time();
    LOG(INFO) << "Enqueue max:           " << timings_.back().enqueue_time();

    std::sort(timings_.begin(), timings_.end(),
              [](const auto &l, const auto &r) {
                return l.dequeue_time() < r.dequeue_time();
              });
    LOG(INFO) << "Dequeue p50:           "
              << timings_[timings_.size() * 0.5].dequeue_time();
    LOG(INFO) << "Dequeue p90:           "
              << timings_[timings_.size() * 0.9].dequeue_time();
    LOG(INFO) << "Dequeue p95:           "
              << timings_[timings_.size() * 0.95].dequeue_time();
    LOG(INFO) << "Dequeue p99:           "
              << timings_[timings_.size() * 0.99].dequeue_time();
    LOG(INFO) << "Dequeue max:           " << timings_.back().dequeue_time();

    std::sort(timings_.begin(), timings_.end(),
              [](const auto &l, const auto &r) {
                return l.total_time() < r.total_time();
              });
    LOG(INFO) << "Total p50:             "
              << timings_[timings_.size() * 0.5].total_time();
    LOG(INFO) << "Total p90:             "
              << timings_[timings_.size() * 0.9].total_time();
    LOG(INFO) << "Total p95:             "
              << timings_[timings_.size() * 0.95].total_time();
    LOG(INFO) << "Total p99:             "
              << timings_[timings_.size() * 0.99].total_time();
    LOG(INFO) << "Total max:             " << timings_.back().total_time();
  }

private:
  int64_t timeval_to_usec(const struct timeval &tv) {
    return tv.tv_sec * 1'000'000 + tv.tv_usec;
  }

  int64_t total_rusage_cpu_usec(const struct rusage &r) {
    return timeval_to_usec(r.ru_utime) + timeval_to_usec(r.ru_stime);
  }

  struct TimingMessage {
    Timing *timing;
  };
  static_assert(offsetof(TimingMessage, timing) == 0);

  void Dequeuer() {
    for (auto e = queue_.Dequeue(); e.ok(); e = queue_.Dequeue()) {
      const TimingMessage *m = std::launder(
          reinterpret_cast<const TimingMessage *>(e->content().data()));
      Timing *t = m->timing;
      t->dequeued = absl::Now();
      { auto to_discard = std::move(e).value(); }
      t->released = absl::Now();
      completions_[t->completion_index].Notify();
    }
  }

  void Enqueuer() {
    int id = next_enqueuer_.fetch_add(1, std::memory_order_relaxed);

    absl::BitGen bitgen;
    auto msg_it = msgs_start_;
    absl::Time next_op_start = absl::Now();

    for (int64_t op = next_op_.fetch_add(1, std::memory_order_relaxed);
         op < timings_.size();
         op = next_op_.fetch_add(1, std::memory_order_relaxed)) {
      absl::SleepFor(next_op_start - absl::Now());
      next_op_start =
          next_op_start + absl::Seconds(absl::Exponential(bitgen, lambda_));
      // N.B. this takes a copy, which could be expensive.
      MessageTypes msg = *msg_it;
      ++msg_it;
      if (msg_it == msgs_limit_) {
        msg_it = msgs_start_;
      }

      Timing &t = timings_[op];
      t.start = absl::Now();
      t.enqueuer = id;
      t.completion_index = op;

      std::visit(
          [&](auto &&m) {
            m.timing = &t;
            t.msg_size = sizeof(decltype(m));
            CHECK_OK(queue_.Enqueue(std::string_view(
                reinterpret_cast<char *>(&m), sizeof(decltype(m)))));
            t.enqueued = absl::Now();
          },
          msg);
    }
  }

  double lambda_;

  decltype(kMsgs)::const_iterator msgs_start_;
  decltype(kMsgs)::const_iterator msgs_limit_;

  ThreadSafeQueue queue_;

  std::atomic<int> next_enqueuer_;
  std::atomic<int64_t> next_op_;
  std::vector<Timing> timings_;
  std::vector<absl::Notification> completions_;

  std::vector<std::thread> dequeuers_;
  std::vector<std::thread> enqueuers_;

  absl::Duration run_wall_time_;
  absl::Duration run_cpu_time_;
};

} // namespace
} // namespace cancellable_queue

using cancellable_queue::Benchmark;

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

#ifndef NDEBUG
  LOG(ERROR) << "non-optimized binary. Consider building with `-c opt`"
             << std::endl;
#endif

  Benchmark b(Benchmark::Options::Make(
      absl::GetFlag(FLAGS_measurements), absl::GetFlag(FLAGS_queue_pages),
      absl::GetFlag(FLAGS_dequeuers), absl::GetFlag(FLAGS_enqueuers),
      absl::GetFlag(FLAGS_min_msg_size), absl::GetFlag(FLAGS_limit_msg_size),
      absl::GetFlag(FLAGS_enqueuer_mean_interarrival_time)));
  b.Run();
  b.Report();
  return 0;
}