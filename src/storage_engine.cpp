#include "flexql/storage_engine.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace flexql {

Page::Page()
    : page_id(0), row_count(0), free_space_offset(0), row_size_bytes(0), reserved{0}, body{} {}

StorageEngine::StorageEngine() : fd_(-1), read_count_(0), write_count_(0), cached_page_count_(0) {}

StorageEngine::~StorageEngine() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool StorageEngine::open(const std::string& filepath) {
  std::filesystem::path p(filepath);
  std::error_code ec;
  std::filesystem::create_directories(p.parent_path(), ec);
  if (ec) {
    return false;
  }

  fd_ = ::open(filepath.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) {
    return false;
  }

  filepath_ = filepath;

  // Cache page count from file size to avoid repeated fstat calls.
  struct stat st;
  if (::fstat(fd_, &st) == 0 && st.st_size >= 0) {
    cached_page_count_.store(
        static_cast<uint32_t>(st.st_size / static_cast<off_t>(kPageSizeBytes)),
        std::memory_order_relaxed);
  }
  return true;
}

bool StorageEngine::write_full(const void* data, size_t len, uint64_t offset) const {
  const char* ptr = static_cast<const char*>(data);
  size_t written = 0;
  while (written < len) {
    const ssize_t rc = ::pwrite(fd_, ptr + written, len - written,
                                static_cast<off_t>(offset + written));
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    written += static_cast<size_t>(rc);
  }
  return true;
}

bool StorageEngine::read_full(void* data, size_t len, uint64_t offset) const {
  char* ptr = static_cast<char*>(data);
  size_t read_total = 0;
  while (read_total < len) {
    const ssize_t rc = ::pread(fd_, ptr + read_total, len - read_total,
                               static_cast<off_t>(offset + read_total));
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (rc == 0) {
      return false;
    }
    read_total += static_cast<size_t>(rc);
  }
  return true;
}

bool StorageEngine::readPage(uint32_t page_id, Page& out_page) const {
  if (fd_ < 0) {
    return false;
  }
  const uint64_t offset = static_cast<uint64_t>(page_id) * kPageSizeBytes;
  const bool ok = read_full(&out_page, sizeof(Page), offset);
  if (ok) {
    read_count_.fetch_add(1, std::memory_order_relaxed);
  }
  return ok;
}

bool StorageEngine::readPages(uint32_t start_page_id, uint32_t count, void* buffer) const {
  if (fd_ < 0 || count == 0) {
    return false;
  }
  const uint64_t offset = static_cast<uint64_t>(start_page_id) * kPageSizeBytes;
  const size_t bytes = static_cast<size_t>(count) * kPageSizeBytes;
  const bool ok = read_full(buffer, bytes, offset);
  if (ok) {
    read_count_.fetch_add(count, std::memory_order_relaxed);
  }
  return ok;
}

bool StorageEngine::writePage(uint32_t page_id, const Page& page) const {
  if (fd_ < 0) {
    return false;
  }
  const uint64_t offset = static_cast<uint64_t>(page_id) * kPageSizeBytes;
  const bool ok = write_full(&page, sizeof(Page), offset);
  if (ok) {
    write_count_.fetch_add(1, std::memory_order_relaxed);
  }
  return ok;
}

uint32_t StorageEngine::pageCount() const {
  return cached_page_count_.load(std::memory_order_relaxed);
}

void StorageEngine::setCachedPageCount(uint32_t count) {
  cached_page_count_.store(count, std::memory_order_relaxed);
}

uint32_t StorageEngine::allocateNewPage(uint16_t row_size_bytes) {
  const uint32_t page_id = cached_page_count_.load(std::memory_order_relaxed);
  Page page;
  page.page_id = page_id;
  page.row_count = 0;
  page.free_space_offset = 0;
  page.row_size_bytes = row_size_bytes;
  std::memset(page.reserved, 0, sizeof(page.reserved));
  if (!writePage(page_id, page)) {
    return UINT32_MAX;
  }
  cached_page_count_.fetch_add(1, std::memory_order_relaxed);
  return page_id;
}

uint32_t StorageEngine::allocateNewPages(uint16_t row_size_bytes, uint32_t count) {
  if (count == 0) return UINT32_MAX;
  const uint32_t first_page_id = cached_page_count_.load(std::memory_order_relaxed);
  // Extend file in one ftruncate to avoid per-page pwrite for empty pages.
  const uint64_t new_size = static_cast<uint64_t>(first_page_id + count) * kPageSizeBytes;
  if (::ftruncate(fd_, static_cast<off_t>(new_size)) != 0) {
    return UINT32_MAX;
  }
  cached_page_count_.store(first_page_id + count, std::memory_order_relaxed);
  return first_page_id;
}

uint64_t StorageEngine::readCount() const {
  return read_count_.load(std::memory_order_relaxed);
}

uint64_t StorageEngine::writeCount() const {
  return write_count_.load(std::memory_order_relaxed);
}

void StorageEngine::resetIoCounters() {
  read_count_.store(0, std::memory_order_relaxed);
  write_count_.store(0, std::memory_order_relaxed);
}

bool StorageEngine::fsyncFile() const {
  if (fd_ < 0) {
    return false;
  }
#ifdef __APPLE__
  return ::fsync(fd_) == 0;
#else
  return ::fdatasync(fd_) == 0;
#endif
}

}  // namespace flexql
