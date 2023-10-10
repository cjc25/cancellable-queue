#ifndef CANCELLABLE_QUEUE_LIB_THREAD_SAFE_QUEUE_H_
#define CANCELLABLE_QUEUE_LIB_THREAD_SAFE_QUEUE_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "lib/ring.h"
#include <cstdint>

namespace cancellable_queue {

// ThreadSafeQueue is a queue which can handle multiple concurrent producers and
// consumers. There is a total order across enqueues. Each enqueued message is
// only dequeued once. Data in the queue is referenced in-place via views into
// the queue's memory represented by ThreadSafeQueue::Entry objects.
class ThreadSafeQueue {
public:
  static absl::StatusOr<ThreadSafeQueue> Make(int64_t capacity);

  ThreadSafeQueue(ThreadSafeQueue &&);
  ThreadSafeQueue &operator=(ThreadSafeQueue &&);

  ~ThreadSafeQueue() = default;

  ThreadSafeQueue(const ThreadSafeQueue &) = delete;
  ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;

  // After Cancel is called, eventually all Enqueue and Dequeue operations will
  // return a CANCELLED error.
  void Cancel();

  // Enqueue pushes `content` to the back of the queue, blocking until space is
  // available if necessary.
  //
  // Returns:
  //   OK - `content` was enqueued
  //   INVALID_ARGUMENT - `content` can never be enqueued
  //   CANCELLED - this queue has been cancelled
  absl::Status Enqueue(std::string_view content);

private:
  struct Header;

public:
  // An Entry is a view into the queue which provides access to record contents.
  // The queue which generates an Entry must outlive the Entry.
  class Entry {
  public:
    Entry(Entry &&);
    Entry &operator=(Entry &&);

    ~Entry();

    Entry(const Entry &) = delete;
    Entry &operator=(const Entry &) = delete;

    std::string_view content() const;

  private:
    friend class ThreadSafeQueue;
    explicit Entry(ThreadSafeQueue *queue, int64_t offset, Header *header);

    ThreadSafeQueue *queue_;
    int64_t offset_;
    Header *header_;
  };

  // Dequeue gets the next entry from the back of the queue, blocking until an
  // entry is available. Live Entry objects pin space in the queue, and the
  // queue must outlive all Entry objects derived from it.
  //
  // Returns:
  //   OK - the next content
  //   CANCELLED - this queue has been cancelled
  absl::StatusOr<Entry> Dequeue();

private:
  explicit ThreadSafeQueue(Ring &&ring);

  absl::Mutex mu_;

  int64_t space_available_ ABSL_GUARDED_BY(mu_);
  int64_t back_ ABSL_GUARDED_BY(mu_);
  int64_t front_ ABSL_GUARDED_BY(mu_);
  // Because entries may be released out of order, the next entry to dequeue
  // might not be at the front of the queue.
  int64_t next_dequeue_ ABSL_GUARDED_BY(mu_);

  bool cancelled_ ABSL_GUARDED_BY(mu_);

  Ring ring_ ABSL_GUARDED_BY(mu_);
};

} // namespace cancellable_queue

#endif // CANCELLABLE_QUEUE_LIB_THREAD_SAFE_QUEUE_H_