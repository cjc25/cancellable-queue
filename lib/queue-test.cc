#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/notification.h"
#include "lib/thread-safe-queue.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <string_view>
#include <thread>

namespace cancellable_queue {
namespace {

using ::testing::Contains;
using ::testing::Eq;
using ::testing::Property;

size_t PageSize() {
  static const size_t kPageSize = sysconf(_SC_PAGE_SIZE);
  return kPageSize;
}

MATCHER(IsOk, "") {
  if (arg.ok()) {
    return true;
  }
  *result_listener << "status: " << arg;
  return false;
}

MATCHER_P(IsOkAndHolds, matcher, "") {
  if (arg.ok()) {
    return ExplainMatchResult(matcher, arg.value(), result_listener);
  }
  *result_listener << "status: " << arg.status();
  return false;
}

MATCHER_P(HasStatusCode, matcher, "") {
  *result_listener << "status: " << arg;
  return ExplainMatchResult(matcher, arg.code(), result_listener);
}

TEST(QueueTest, ConsumeAll) {
  constexpr std::string_view kRecord = "7 bytes";

  // Make sure we can tolerate at least 10 records in the queue. This should
  // ~always be 1 4K+ page.
  int64_t pages = ((10 * kRecord.size()) / PageSize()) + 1;
  int64_t capacity = pages * PageSize();

  // Enqueue enough records to wrap the queue three times. Any overhead in the
  // queue representation will make this wrap more.
  int64_t record_count = ((3 * capacity) / kRecord.size()) + 1;

  auto q = ThreadSafeQueue::Make(capacity);
  ASSERT_THAT(q.status(), IsOk());

  absl::Notification dequeue_done;

  std::thread dequeuer([&]() {
    for (int i = 0; i < record_count; ++i) {
      auto e = q->Dequeue();
      EXPECT_THAT(
          e, IsOkAndHolds(Property("content", &ThreadSafeQueue::Entry::content,
                                   Eq(kRecord))));
    }
    dequeue_done.Notify();
    EXPECT_THAT(q->Dequeue().status(),
                HasStatusCode(absl::StatusCode::kCancelled));
  });

  for (int64_t i = 0; i < record_count; ++i) {
    EXPECT_THAT(q->Enqueue(kRecord), IsOk());
  }

  dequeue_done.WaitForNotification();
  q->Cancel();
  dequeuer.join();
}

TEST(QueueTest, ConsumeSome) {
  const std::string kRecord(PageSize(), 'a');
  constexpr int kEnqueueRecords = 10;
  constexpr int kDequeueRecords = kEnqueueRecords / 2;

  // We can tolerate slightly fewer than kEnqueueRecords in the queue due to
  // overheads. Assuming overheads per record are less than a full page size, we
  // can tolerate more than half that many records in the queue, so this test
  // will not deadlock.
  auto q = ThreadSafeQueue::Make(PageSize() * kEnqueueRecords);
  ASSERT_THAT(q.status(), IsOk());

  absl::Notification dequeue_done;
  absl::Notification cancelled;

  std::thread dequeuer([&]() {
    for (int i = 0; i < kDequeueRecords; ++i) {
      auto e = q->Dequeue();
      EXPECT_THAT(
          e, IsOkAndHolds(Property("content", &ThreadSafeQueue::Entry::content,
                                   Eq(std::string_view(kRecord)))));
    }
    dequeue_done.Notify();
    cancelled.WaitForNotification();
    EXPECT_THAT(q->Dequeue().status(),
                HasStatusCode(absl::StatusCode::kCancelled));
  });

  for (int64_t i = 0; i < kEnqueueRecords; ++i) {
    EXPECT_THAT(q->Enqueue(kRecord), IsOk());
  }

  dequeue_done.WaitForNotification();
  q->Cancel();
  cancelled.Notify();
  dequeuer.join();
}

TEST(QueueTest, MultipleEnqueuersAndDequeuers) {
  constexpr auto kRecords = std::to_array<std::string_view>({
      "this is a record",
      "this is yet another record",
      "and a third record for us to enter",
      "one thing",
      "and another",
  });

  auto q = ThreadSafeQueue::Make(PageSize() * 100);
  ASSERT_THAT(q.status(), IsOk());

  std::vector<std::thread> enqueuers;
  constexpr int kPerThreadEnqueues = 100'000 / kRecords.size();
  std::transform(kRecords.begin(), kRecords.end(),
                 std::back_inserter(enqueuers), [&](std::string_view record) {
                   return std::thread([&, record]() {
                     for (int i = 0; i < kPerThreadEnqueues; ++i) {
                       EXPECT_THAT(q->Enqueue(record), IsOk());
                     }
                   });
                 });

  absl::BlockingCounter dequeue_ct(kPerThreadEnqueues * enqueuers.size());
  std::vector<std::thread> dequeuers;
  std::generate_n(std::back_inserter(dequeuers), kRecords.size(), [&]() {
    return std::thread([&]() {
      auto r = q->Dequeue();
      for (; r.ok(); r = q->Dequeue()) {
        EXPECT_THAT(kRecords, Contains(r->content()));
        dequeue_ct.DecrementCount();
      }
      EXPECT_THAT(r.status(), HasStatusCode(absl::StatusCode::kCancelled));
    });
  });

  dequeue_ct.Wait();
  q->Cancel();
  std::for_each(enqueuers.begin(), enqueuers.end(),
                [](std::thread &t) { t.join(); });
  std::for_each(dequeuers.begin(), dequeuers.end(),
                [](std::thread &t) { t.join(); });
}

TEST(QueueTest, RecordTooBig) {
  auto q = ThreadSafeQueue::Make(PageSize());
  ASSERT_THAT(q.status(), IsOk());
  const std::string kRecord(PageSize() + 1, 'a');
  EXPECT_THAT(q->Enqueue(kRecord),
              HasStatusCode(absl::StatusCode::kInvalidArgument));
}

} // namespace
} // namespace cancellable_queue