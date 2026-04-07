#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

namespace flexql {

constexpr size_t kPageSizeBytes = 4096;
constexpr size_t kPageHeaderBytes = 16;
constexpr size_t kPageBodyBytes = kPageSizeBytes - kPageHeaderBytes;

struct __attribute__((aligned(8))) Page {
  uint32_t page_id;
  uint16_t row_count;
  uint16_t free_space_offset;
  uint16_t row_size_bytes;
  char reserved[6];
  std::array<char, kPageBodyBytes> body;

  Page();
};

static_assert(sizeof(Page) == kPageSizeBytes, "Page must be exactly 4096 bytes");

class StorageEngine {
 public:
  StorageEngine();
  ~StorageEngine();

  bool open(const std::string& filepath);
  bool readPage(uint32_t page_id, Page& out_page) const;
  bool writePage(uint32_t page_id, const Page& page) const;
  bool readPages(uint32_t start_page_id, uint32_t count, void* buffer) const;
  uint32_t allocateNewPage(uint16_t row_size_bytes);
  uint32_t allocateNewPages(uint16_t row_size_bytes, uint32_t count);
  uint32_t pageCount() const;
  void setCachedPageCount(uint32_t count);
  uint64_t readCount() const;
  uint64_t writeCount() const;
  void resetIoCounters();
  bool fsyncFile() const;
  bool write_full(const void* data, size_t len, uint64_t offset) const;
  bool read_full(void* data, size_t len, uint64_t offset) const;

 private:
  int fd_;
  std::string filepath_;
  mutable std::atomic<uint64_t> read_count_;
  mutable std::atomic<uint64_t> write_count_;
  mutable std::atomic<uint32_t> cached_page_count_;
};

}  // namespace flexql
