#include "lib/thread-safe-queue.h"
#include "absl/log/absl_check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lib/ring.h"
#include <string_view>

namespace cancellable_queue {
namespace {

constexpr size_t aligned_size(size_t size) {
  constexpr size_t kAlignment = alignof(std::max_align_t);

  if (size % kAlignment == 0) {
    return size;
  } else {
    return size + (kAlignment - (size % kAlignment));
  }
}

} // namespace

struct ThreadSafeQueue::Header {
  static constexpr int64_t kLive = 1 << 0;
  static constexpr int64_t kReserved = 1 << 1;

  int64_t flags;
  size_t length;
  alignas(std::max_align_t) char data[];

  bool live() const { return flags & kLive; }
  void set_live() { flags |= kLive; }

  bool empty() const { return flags == 0; }
  void clear_flags() { flags = 0; }

  static size_t size(size_t data_length) {
    return aligned_size(sizeof(Header)) + aligned_size(data_length);
  }
  size_t size() { return Header::size(length); }
};

absl::StatusOr<ThreadSafeQueue> ThreadSafeQueue::Make(int64_t capacity) {
  auto ring = Ring::Make(capacity);
  if (!ring.ok()) {
    return ring.status();
  }

  return ThreadSafeQueue(std::move(ring).value());
}

ThreadSafeQueue::ThreadSafeQueue(Ring &&ring)
    : mu_(), space_available_(ring.capacity()), back_(0), front_(0),
      next_dequeue_(0), cancelled_(false), ring_(std::move(ring)) {}

ThreadSafeQueue::ThreadSafeQueue(ThreadSafeQueue &&o)
    : mu_(), space_available_(o.space_available_), back_(o.back_),
      front_(o.front_), next_dequeue_(o.next_dequeue_),
      cancelled_(o.cancelled_), ring_(std::move(o.ring_)) {}

ThreadSafeQueue &ThreadSafeQueue::operator=(ThreadSafeQueue &&o) {
  // Clean up any existing queue when `old` is destroyed.
  ThreadSafeQueue old(std::move(*this));

  // This can deadlock if the application concurrently move-assigns two
  // ThreadSafeQueues to each other. That's a pretty silly thing to do, so I
  // don't really mind.
  absl::MutexLock l(&mu_);
  absl::MutexLock ol(&o.mu_);
  space_available_ = o.space_available_;
  back_ = o.back_;
  front_ = o.front_;
  next_dequeue_ = o.next_dequeue_;
  cancelled_ = o.cancelled_;
  ring_ = std::move(o.ring_);

  return *this;
}

void ThreadSafeQueue::Cancel() {
  absl::MutexLock l(&mu_);
  cancelled_ = true;
}

absl::Status ThreadSafeQueue::Enqueue(std::string_view content) {
  if (content.length() == 0) {
    return absl::InvalidArgumentError("0-length content cannot be enqueued");
  }

  absl::MutexLock l(&mu_);
  auto size = Header::size(content.length());
  if (size > ring_.capacity()) {
    return absl::InvalidArgumentError(
        absl::StrCat("contents of length ", content.length(),
                     " too big for ring of capacity ", ring_.capacity()));
  }

  auto space_available_or_cancelled = [this,
                                       size]() ABSL_SHARED_LOCKS_REQUIRED(mu_) {
    return cancelled_ || space_available_ >= size;
  };
  mu_.Await(absl::Condition(&space_available_or_cancelled));

  // No new pushes to a cancelled queue.
  if (cancelled_) {
    return absl::CancelledError();
  }

  auto header = ring_.ObjectAtOffset<Header>(back_);
  if (!header.ok()) {
    return header.status();
  }
  **header = {.flags = Header::kReserved, .length = content.length()};
  back_ += size;
  space_available_ -= size;

  mu_.Unlock();
  content.copy((*header)->data, content.length());
  mu_.Lock();
  (*header)->set_live();

  return absl::OkStatus();
}

std::string_view ThreadSafeQueue::Entry::content() const {
  return std::string_view(header_->data, header_->length);
}

ThreadSafeQueue::Entry::Entry(ThreadSafeQueue::Entry &&o)
    : queue_(o.queue_), offset_(o.offset_), header_(o.header_) {
  o.header_ = nullptr;
}

ThreadSafeQueue::Entry &ThreadSafeQueue::Entry::operator=(Entry &&o) {
  // Clean up this entry's current ref when `old` is destroyed.
  Entry old(std::move(*this));

  queue_ = o.queue_;
  offset_ = o.offset_;
  header_ = o.header_;
  o.header_ = nullptr;
  return *this;
}

ThreadSafeQueue::Entry::~Entry() {
  if (header_ == nullptr) {
    return;
  }

  absl::MutexLock l(&queue_->mu_);
  header_->clear_flags();

  if (offset_ != queue_->front_) {
    // Still waiting for an earlier entry to be cleaned up, so do nothing.
    return;
  }

  // We have to clean up this entry and any no-longer-needed entries ahead of
  // us. The end of the queue looks like an all-0 entry
  while (header_->empty() && header_->length > 0) {
    offset_ += header_->size();
    queue_->space_available_ += header_->size();
    queue_->front_ += header_->size();

    // This indicates a memory corruption bug in the queue.
    ABSL_CHECK_LE(queue_->front_, queue_->next_dequeue_)
        << "queue front unexpectedly ahead of next_dequeue";

    memset(header_, 0, header_->size());
    // This would crash if we didn't align Headers in the queue correctly.
    // That's better than SIGBUS or silently tearing objects!
    header_ = queue_->ring_.ObjectAtOffset<Header>(offset_).value();
  }
}

ThreadSafeQueue::Entry::Entry(ThreadSafeQueue *queue, int64_t offset,
                              Header *header)
    : queue_(queue), offset_(offset), header_(header) {}

absl::StatusOr<ThreadSafeQueue::Entry> ThreadSafeQueue::Dequeue() {
  absl::MutexLock l(&mu_);

  auto entry_available_or_cancelled = [this]() ABSL_SHARED_LOCKS_REQUIRED(mu_) {
    return cancelled_ || next_dequeue_ < back_;
  };
  mu_.Await((absl::Condition(&entry_available_or_cancelled)));

  if (cancelled_) {
    return absl::CancelledError();
  }

  auto header = ring_.ObjectAtOffset<Header>(next_dequeue_).value();
  Entry e(this, next_dequeue_, header);

  next_dequeue_ += e.header_->size();
  ABSL_CHECK_LE(next_dequeue_, back_)
      << "next_dequeue unexpectedly ahead of queue back";

  mu_.Await(absl::Condition(header, &Header::live));

  return e;
}

} // namespace cancellable_queue