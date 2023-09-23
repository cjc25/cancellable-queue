#include "absl/cleanup/cleanup.h"
#include "lib/ring.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstddef>
#include <limits>
#include <sys/resource.h>
#include <unistd.h>

namespace cancellable_queue {
namespace {

using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::ExplainMatchResult;
using ::testing::Gt;
using ::testing::Pointee;
using ::testing::StrEq;

size_t PageSize() {
  static const size_t kPageSize = sysconf(_SC_PAGE_SIZE);
  return kPageSize;
}

MATCHER(IsOk, "") {
  if (arg.ok()) {
    return true;
  }
  *result_listener << "status: " << arg.status();
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
  *result_listener << "status: " << arg.status();
  return ExplainMatchResult(matcher, arg.status().code(), result_listener);
}

TEST(RingTest, CapacityOK) { ASSERT_THAT(Ring::Make(PageSize()), IsOk()); }

TEST(RingTest, CapacityBad) {
  // Surely PAGE_SIZE isn't 1 byte....
  ASSERT_THAT(PageSize(), Gt(1));
  ASSERT_THAT(Ring::Make(PageSize() + 1),
              HasStatusCode(absl::StatusCode::kInvalidArgument));
}

TEST(RingTest, ZeroInit) {
  auto ring = Ring::Make(PageSize());
  ASSERT_THAT(ring, IsOk());

  // The ring is initialized to 0
  char expected[100] = {0};
  EXPECT_THAT(ring->ObjectAtOffset<char[100]>(0),
              IsOkAndHolds(Pointee(ElementsAreArray(expected))));
}

TEST(RingTest, OffsetLoad) {
  auto ring = Ring::Make(PageSize());
  ASSERT_THAT(ring, IsOk());

  auto start = ring->ObjectAtOffset<char[100]>(0);
  ASSERT_THAT(start, IsOk());

  const char some_text[] = "tendon tender";
  strncpy(**start, some_text, sizeof(some_text));

  auto later = ring->ObjectAtOffset<char[100]>(3);
  ASSERT_THAT(later, IsOk());

  EXPECT_THAT(**later, StrEq("don tender"));
  ***later = '\0';
  EXPECT_THAT(**start, StrEq("ten"));
}

TEST(RingTest, WrappedLoad) {
  auto ring = Ring::Make(PageSize());
  ASSERT_THAT(ring, IsOk());

  auto start = ring->ObjectAtOffset<char[100]>(PageSize() - 3);
  ASSERT_THAT(start, IsOk());

  const char some_text[] = "tendon tender";
  strncpy(**start, some_text, sizeof(some_text));

  auto later = ring->ObjectAtOffset<char[100]>(4);
  ASSERT_THAT(later, IsOk());

  EXPECT_THAT(**later, StrEq("tender"));
  ***later = '\0';
  EXPECT_THAT(**start, StrEq("tendon "));
}

// Don't allow unaligned loads of the specified type. On some platforms, this
// can SIGBUS. On others, atomic operations can be silently torn!
TEST(RingTest, Unaligned) {
  auto ring = Ring::Make(PageSize());
  ASSERT_THAT(ring, IsOk());

  EXPECT_THAT(ring->ObjectAtOffset<std::atomic<int64_t>>(1),
              HasStatusCode(absl::StatusCode::kInvalidArgument));
}

TEST(RingTest, BigRing) {
  // On a 4K page platform, this is 4MiB.
  ASSERT_THAT(Ring::Make(PageSize() * (1 << 10)), IsOk());
}

TEST(RingTest, InvalidCapacities) {
  // This is the largest int64_t which passes argument checks, but would attempt
  // to mmap ~8EiB of virtual memory. That should fail.
  int64_t capacity = std::numeric_limits<int64_t>::max() / 2;
  capacity = capacity - (capacity % PageSize());
  EXPECT_THAT(Ring::Make(capacity),
              HasStatusCode(absl::StatusCode::kResourceExhausted));

  // The largest int64_t which is PageSize() aligned. This value can't be
  // addressed with a signed integer, and we don't want noise for folks who
  // compile with signed integer overflow warnings enabled.
  capacity = std::numeric_limits<int64_t>::max();
  capacity = capacity - (capacity % PageSize());
  EXPECT_THAT(Ring::Make(capacity),
              HasStatusCode(absl::StatusCode::kInvalidArgument));

  // Negative capacities can't be allocated
  capacity = -PageSize();
  EXPECT_THAT(Ring::Make(capacity),
              HasStatusCode(absl::StatusCode::kInvalidArgument));
}

TEST(RingTest, MoveAssignment) {
  // Shrink the address space
  struct rlimit prev_rlimit;
  ASSERT_THAT(getrlimit(RLIMIT_AS, &prev_rlimit), Eq(0));
  struct rlimit new_rlimit = {
      .rlim_cur = std::min<rlim_t>(1 << 30, prev_rlimit.rlim_max),
      .rlim_max = prev_rlimit.rlim_max,
  };
  ASSERT_THAT(setrlimit(RLIMIT_AS, &new_rlimit), Eq(0));
  auto reset_rlimit = absl::MakeCleanup(
      [&]() { ASSERT_THAT(setrlimit(RLIMIT_AS, &prev_rlimit), Eq(0)); });

  int64_t capacity = 50 << 20;

  // Each ring is 50MiB, but we can only map 1GiB of virtual address space
  // (which is hopefully more than we had in use...). By looping over 1000
  // rings, we would have blasted past that if we didn't clean up old rings on
  // move-assignment.
  //
  // This is faster and more explicit than writing each page to force an oom,
  // although it'll be brittle if somehow this tiny test binary grows beyond
  // 1GiB of allocated virtual address space!
  auto ring = Ring::Make(capacity);
  ASSERT_THAT(ring, IsOk());
  for (int i = 0; i < 1024; ++i) {
    ring = Ring::Make(capacity);
    ASSERT_THAT(ring, IsOk());
  }
}

} // namespace
} // namespace cancellable_queue