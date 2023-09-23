#include "lib/ring.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <sys/mman.h>
#include <unistd.h>

namespace cancellable_queue {
namespace {

class FdCloser {
public:
  explicit FdCloser(int fd) : fd_(fd) {}
  ~FdCloser() {
    if (fd_ == -1) {
      return;
    }
    close(fd_); // N.B. we ignore any error.
  }

  int fd() { return fd_; };

private:
  int fd_;
};

} // namespace

absl::StatusOr<Ring> Ring::Make(int64_t capacity) {
  if (capacity <= 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("capacity ", capacity, " must be positive"));
  }

  if ((std::numeric_limits<int64_t>::max() / 2) < capacity) {
    return absl::InvalidArgumentError(
        absl::StrCat("capacity ", capacity, " will overflow!"));
  }

  static size_t kPageSize = sysconf(_SC_PAGESIZE);
  if (capacity % kPageSize) {
    return absl::InvalidArgumentError(absl::StrCat(
        "capacity ", capacity, " is not a multiple of page size ", kPageSize));
  }

  // Reserve a virtual memory range to place the ring in.
  char *data = static_cast<char *>(mmap(nullptr, capacity * 2, PROT_NONE,
                                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  if (data == MAP_FAILED) {
    return absl::ErrnoToStatus(
        errno,
        absl::StrCat("failed to map the ring range with capacity ", capacity));
  }

  // Construct a memfd to be mapped into the reserved range twice.
  FdCloser memfd(memfd_create("ring", 0));
  if (memfd.fd() == -1) {
    return absl::ErrnoToStatus(errno,
                               absl::StrCat("failed to create ring memfd"));
  }

  if (ftruncate(memfd.fd(), capacity)) {
    return absl::ErrnoToStatus(errno,
                               absl::StrCat("failed to extend ring memfd"));
  }

  if (mmap(data, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED,
           memfd.fd(), 0) == MAP_FAILED) {
    return absl::ErrnoToStatus(
        errno, absl::StrCat("failed to map the front half of the ring"));
  }

  if (mmap(data + capacity, capacity, PROT_READ | PROT_WRITE,
           MAP_SHARED | MAP_FIXED, memfd.fd(), 0) == MAP_FAILED) {
    return absl::ErrnoToStatus(
        errno, absl::StrCat("failed to map the back half of the ring"));
  }

  return Ring(capacity, data);
}

Ring::Ring(int64_t capacity, char *data) : capacity_(capacity), data_(data) {}

Ring::Ring(Ring &&o) : capacity_(o.capacity_), data_(o.data_) {
  o.data_ = static_cast<char *>(MAP_FAILED);
}

Ring &Ring::operator=(Ring &&o) {
  // Clean up any existing ring when `old` is destroyed.
  Ring old(std::move(*this));

  capacity_ = o.capacity_;
  data_ = o.data_;
  o.data_ = static_cast<char *>(MAP_FAILED);
  return *this;
}

Ring::~Ring() {
  if (data_ != MAP_FAILED) {
    munmap(data_, capacity_ * 2);
  }
}

} // namespace cancellable_queue