#include "flexql/insert_buffer.hpp"

#include <cstddef>
#include <vector>

#include "flexql/lock_order.hpp"
#include "flexql/row_codec.hpp"

namespace flexql {

namespace {

bool append_rows_to_pages(Table& table, BufferPool& buffer_pool, uint32_t table_id,
                          const std::vector<Row>& rows,
                          InsertBuffer::RowPlacementCallback callback,
                          void* callback_ctx) {
  if (rows.empty()) {
    return true;
  }

  uint32_t page_id = table.current_page_id;
  BufferFrame* frame = buffer_pool.fetchPage(table_id, page_id);
  if (frame == nullptr) {
    return false;
  }

  bool page_dirty = false;
  for (const Row& row : rows) {
    if (static_cast<uint32_t>(frame->page.free_space_offset) + table.row_size_bytes > kPageBodyBytes) {
      if (page_dirty) {
        buffer_pool.markDirty(table_id, page_id);
      }
      buffer_pool.unpinPage(table_id, page_id);

      const uint32_t new_page = table.storage->allocateNewPage(table.row_size_bytes);
      if (new_page == UINT32_MAX) {
        return false;
      }

      table.current_page_id = new_page;
      page_id = new_page;
      frame = buffer_pool.fetchPage(table_id, page_id);
      if (frame == nullptr) {
        return false;
      }
      page_dirty = false;
    }

    const uint16_t row_offset = frame->page.free_space_offset;
    if (!serializeRowIntoPage(row, table.schema, table.row_size_bytes, frame->page)) {
      buffer_pool.unpinPage(table_id, page_id);
      return false;
    }
    if (callback != nullptr) {
      callback(callback_ctx, row, page_id, row_offset);
    }
    page_dirty = true;
  }

  if (page_dirty) {
    buffer_pool.markDirty(table_id, page_id);
  }
  buffer_pool.unpinPage(table_id, page_id);
  return true;
}

}  // namespace

InsertBuffer::InsertBuffer(size_t capacity) : capacity_(capacity), buffer_(), buffer_index_(), mtx_() {
  buffer_.reserve(capacity_);
  buffer_index_.reserve(capacity_);
}

bool InsertBuffer::append(const Row& row, int64_t pk) {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  buffer_.emplace_back(row);
  buffer_index_[pk] = &buffer_.back();
  return true;
}

bool InsertBuffer::append(Row&& row, int64_t pk) {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  buffer_.emplace_back(std::move(row));
  buffer_index_[pk] = &buffer_.back();
  return true;
}

bool InsertBuffer::shouldFlush() const {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  return buffer_.size() >= capacity_;
}

size_t InsertBuffer::size() const {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  return buffer_.size();
}

void InsertBuffer::clear() {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  buffer_.clear();
  buffer_index_.clear();
}

bool InsertBuffer::findByPrimaryKey(int64_t pk, Row& out_row) const {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  const auto it = buffer_index_.find(pk);
  if (it == buffer_index_.end()) {
    return false;
  }
  out_row = *(it->second);
  return true;
}

bool InsertBuffer::containsPrimaryKey(int64_t pk) const {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  return buffer_index_.find(pk) != buffer_index_.end();
}

bool InsertBuffer::flush(Table& table,
                         BufferPool& buffer_pool,
                         uint32_t table_id,
                         RowPlacementCallback callback,
                         void* callback_ctx) {
  DebugLockLevelGuard level_guard(LockLevel::INSERT_BUFFER);
  std::lock_guard<std::mutex> lock(mtx_);
  return flush_locked(table, buffer_pool, table_id, callback, callback_ctx);
}

bool InsertBuffer::flush_locked(Table& table,
                                 BufferPool& buffer_pool,
                                 uint32_t table_id,
                                 RowPlacementCallback callback,
                                 void* callback_ctx) {
  if (buffer_.empty()) {
    return true;
  }

  if (!append_rows_to_pages(table, buffer_pool, table_id, buffer_, callback, callback_ctx)) {
    return false;
  }

  // Durability is deferred to the checkpoint mechanism (every kCheckpointBytes)
  // to avoid expensive per-flush fsync calls during high-throughput inserts.

  buffer_.clear();
  buffer_index_.clear();
  return true;
}

}  // namespace flexql
