#ifndef CANCELLABLE_QUEUE_LIB_RING_H_
#define CANCELLABLE_QUEUE_LIB_RING_H_

#include "absl/status/statusor.h"
#include <cstdint>
#include <new>

namespace cancellable_queue {

// Ring is a region of memory mapped into the virtual address space twice, back
// to back.
//
// In particular, we map as follows:
//
//  Allocate a range [A, A + C)
//  Reserve a range [X, X + 2C)
//
//  +-----------------+------------+-----------------+
//  | Allocated space | [A, A + C) | [A, A + C)      |
//  +-----------------+------------+-----------------+
//  | Reserved space  | [X, X + C) | [X + C, X + 2C) |
//  +-----------------+------------+-----------------+
//
// This imposes some alignment and size constraints on `capacity`, based on the
// platform.
//
// Ring does not perform any locking. Non-overlapping offsets can safely be used
// concurrently, but clients must coordinate access to overlappying offsets.
class Ring {
public:
  //
  // Returns:
  //   OK - the allocated ring
  //   INVALID_ARGUMENT - size `capacity` cannot be allocated on this platform
  //   OTHER ERROR - other failure to allocate the ring
  static absl::StatusOr<Ring> Make(int64_t capacity);

  Ring(Ring &&);
  Ring &operator=(Ring &&);

  Ring(const Ring &) = delete;
  Ring &operator=(const Ring &) = delete;

  ~Ring();

  int64_t capacity() const { return capacity_; };

  // ObjectAtOffset returns a pointer to an object T if it can be aliased
  // correctly at `offset` in the ring.
  //
  // Returns:
  //   OK - the pointer to T
  //   INVALID_ARGUMENT - offset is not a safe place for an object of type T.
  template <typename T> absl::StatusOr<T *> ObjectAtOffset(int64_t offset) {
    char *address = data_ + (offset % capacity_);
    if (reinterpret_cast<std::uintptr_t>(address) % alignof(T)) {
      return absl::InvalidArgumentError(
          absl::StrCat("offset ", offset, " (", offset,
                       " in ring) not aligned to ", alignof(T)));
    }

    return std::launder(reinterpret_cast<T *>(address));
  }

private:
  explicit Ring(int64_t capacity, char *data);

  int64_t capacity_;
  char *data_;
};

} // namespace cancellable_queue

#endif // CANCELLABLE_QUEUE_LIB_RING_H_