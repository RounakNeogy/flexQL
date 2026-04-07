#include "flexql/execution_engine.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

#include "flexql/lock_order.hpp"
#include "flexql/row_codec.hpp"

namespace flexql {

namespace {

std::vector<size_t> buildColumnOffsets(const Table& table) {
  std::vector<size_t> offsets(table.schema.columns.size(), 0);
  size_t cursor = kRowHeaderAlignedBytes;
  for (size_t i = 0; i < table.schema.columns.size(); ++i) {
    offsets[i] = cursor;
    cursor += columnStorageSize(table.schema.columns[i].type);
  }
  return offsets;
}

}  // namespace

ExecutionEngine::TableRuntime::TableRuntime()
    : insert_buffer(),
      bulk_mode(false),
      bulk_rows(),
      bulk_index(),
      primary_index(),
      bplus_tree(),
      page_bloom_filters(),
      recent_insert_micros(),
      tombstoned_rows(0),
      live_rows(0),
      pending_insert_bytes(0) {
  bulk_rows.reserve(10000);
  bulk_index.reserve(10000);
  page_bloom_filters.reserve(8192);
}

ExecutionEngine::ExecutionEngine(BufferPool& buffer_pool)
    : buffer_pool_(buffer_pool),
      runtime_(),
      wal_("data/wal.log"),
      insert_counter_(0),
      thread_pool_(),
      expiration_running_(false),
      expiration_thread_(),
      expiration_tables_mutex_(),
      expiration_tables_() {
  wal_.start();
}

ExecutionEngine::~ExecutionEngine() {
  stopExpirationThread();
  wal_.stop();
}

ExecutionEngine::TableRuntime& ExecutionEngine::stateFor(uint32_t table_id) {
  return runtime_[table_id];
}

uint64_t ExecutionEngine::nowMicros() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

bool ExecutionEngine::rowLiveAndFresh(const uint8_t* row_ptr, int64_t now_ms) {
  if (row_ptr[0] == kTombstoneDeleted) {
    return false;
  }
  int64_t expiry = 0;
  std::memcpy(&expiry, row_ptr + 8, sizeof(int64_t));
  return (expiry == 0 || expiry >= now_ms);
}

void ExecutionEngine::readColumnValue(const uint8_t* row_ptr,
                                      size_t offset,
                                      ColType type,
                                      RowValue& out_value) {
  const uint8_t* vptr = row_ptr + offset;
  if (type == ColType::INT) {
    std::memcpy(&out_value.as_int, vptr, sizeof(int64_t));
  } else if (type == ColType::DECIMAL) {
    std::memcpy(&out_value.as_double, vptr, sizeof(double));
  } else if (type == ColType::DATETIME) {
    std::memcpy(&out_value.as_datetime, vptr, sizeof(int64_t));
  } else {
    std::memcpy(&out_value.as_varchar.len, vptr, sizeof(uint16_t));
    std::memcpy(out_value.as_varchar.buf, vptr + 2, 254);
  }
}

bool ExecutionEngine::compareValues(CompareOp op,
                                    ColType type,
                                    const RowValue& lhs,
                                    const RowValue& rhs) {
  if (type == ColType::INT || type == ColType::DATETIME) {
    const int64_t a = (type == ColType::INT) ? lhs.as_int : lhs.as_datetime;
    const int64_t b = (type == ColType::INT) ? rhs.as_int : rhs.as_datetime;
    if (op == CompareOp::EQ) return a == b;
    if (op == CompareOp::NE) return a != b;
    if (op == CompareOp::LT) return a < b;
    if (op == CompareOp::LE) return a <= b;
    if (op == CompareOp::GT) return a > b;
    return a >= b;
  }
  if (type == ColType::DECIMAL) {
    const double a = lhs.as_double;
    const double b = rhs.as_double;
    if (op == CompareOp::EQ) return a == b;
    if (op == CompareOp::NE) return a != b;
    if (op == CompareOp::LT) return a < b;
    if (op == CompareOp::LE) return a <= b;
    if (op == CompareOp::GT) return a > b;
    return a >= b;
  }

  const uint16_t alen = lhs.as_varchar.len;
  const uint16_t blen = rhs.as_varchar.len;
  int cmp = 0;
  if (alen == blen) {
    cmp = std::memcmp(lhs.as_varchar.buf, rhs.as_varchar.buf, alen);
  } else {
    cmp = (alen < blen) ? -1 : 1;
  }
  if (op == CompareOp::EQ) return cmp == 0;
  if (op == CompareOp::NE) return cmp != 0;
  if (op == CompareOp::LT) return cmp < 0;
  if (op == CompareOp::LE) return cmp <= 0;
  if (op == CompareOp::GT) return cmp > 0;
  return cmp >= 0;
}

bool ExecutionEngine::equalValuesForJoin(ColType type, const RowValue& lhs, const RowValue& rhs) {
  return compareValues(CompareOp::EQ, type, lhs, rhs);
}

bool ExecutionEngine::rowPassesCondition(const uint8_t* row_ptr,
                                         const Table& table,
                                         const Condition& cond,
                                         const std::vector<size_t>& col_offsets) {
  const size_t col_idx = static_cast<size_t>(cond.col_index);
  if (col_idx >= col_offsets.size() || col_idx >= table.schema.columns.size()) {
    return false;
  }
  RowValue lhs{};
  readColumnValue(row_ptr, col_offsets[col_idx], cond.col_type, lhs);
  return compareValues(cond.op_code, cond.col_type, lhs, cond.value);
}

void ExecutionEngine::startExpirationThread(const std::vector<std::pair<uint32_t, Table*>>& tables) {
  stopExpirationThread();
  {
    std::lock_guard<std::mutex> lock(expiration_tables_mutex_);
    expiration_tables_ = tables;
  }
  expiration_running_.store(true, std::memory_order_release);
  expiration_thread_ = std::thread([this]() { expirationLoop(); });
}

void ExecutionEngine::stopExpirationThread() {
  const bool was_running = expiration_running_.exchange(false, std::memory_order_acq_rel);
  if (was_running && expiration_thread_.joinable()) {
    expiration_thread_.join();
  }
}

uint64_t ExecutionEngine::bloomKey(uint32_t table_id, uint32_t page_id, uint16_t col_index) {
  return (static_cast<uint64_t>(table_id) << 40U) | (static_cast<uint64_t>(page_id) << 8U) |
         static_cast<uint64_t>(col_index);
}

uint64_t ExecutionEngine::valueHash(ColType type, const RowValue& value) {
  if (type == ColType::INT || type == ColType::DATETIME) {
    return static_cast<uint64_t>(value.as_int) * 11400714819323198485ULL;
  }
  if (type == ColType::DECIMAL) {
    uint64_t bits = 0;
    std::memcpy(&bits, &value.as_double, sizeof(double));
    return bits;
  }
  return xxhash64(value.as_varchar.buf, value.as_varchar.len, 0);
}

void ExecutionEngine::onRowPersisted(const Table& table,
                                      uint32_t table_id,
                                      const Row& row,
                                      uint32_t page_id,
                                      uint16_t row_offset) {
  TableRuntime& state = stateFor(table_id);
  const ColType pk_type = table.schema.columns[0].type;
  const int64_t key = static_cast<int64_t>(encodePrimaryKey(pk_type, row.values[0]));
  const RowValue::VarcharValue* raw_varchar =
      (pk_type == ColType::VARCHAR) ? &row.values[0].as_varchar : nullptr;
  state.bplus_tree.insert(key, RecordPointer{page_id, row_offset}, raw_varchar);

  // Bloom filters are populated lazily during scans or warmup.
  // Skipping here saves ~4 hash+map operations per row during bulk inserts.
}

void ExecutionEngine::maybeEnableAdaptiveBulk(uint32_t table_id) {
  (void)table_id;
  // Disabled by default to avoid deferring huge pending row sets that can stall
  // the first SELECT with a large synchronous flush.
}

bool ExecutionEngine::beginBulkLoad(uint32_t table_id) {
  TableRuntime& state = stateFor(table_id);
  state.bulk_mode = true;
  return true;
}

bool ExecutionEngine::writeBulkLoadWalMarker(const Table& table, size_t row_count) {
  const std::string wal_path = "data/" + table.name + "/wal.log";
  std::filesystem::create_directories(std::filesystem::path(wal_path).parent_path());

  std::ofstream wal(wal_path, std::ios::app);
  if (!wal.good()) {
    return false;
  }
  wal << "BULK_LOAD_COMPLETE|" << table.name << "|" << row_count << "\n";
  return wal.good();
}

void ExecutionEngine::buildPrimaryIndexOnePass(uint32_t table_id, ColType pk_type, const std::vector<Row>& rows) {
  std::vector<std::pair<int64_t, uint64_t>> keys;
  keys.reserve(rows.size());

  for (size_t i = 0; i < rows.size(); ++i) {
    const int64_t encoded = static_cast<int64_t>(encodePrimaryKey(pk_type, rows[i].values[0]));
    keys.emplace_back(encoded, static_cast<uint64_t>(i));
  }

  std::sort(keys.begin(), keys.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

  TableRuntime& state = stateFor(table_id);
  state.primary_index.clear();
  state.primary_index.reserve(keys.size());
  for (const auto& kv : keys) {
    state.primary_index[kv.first] = kv.second;
  }
}

bool ExecutionEngine::appendRowsToPages(Table& table,
                                        uint32_t table_id,
                                        const std::vector<Row>& rows,
                                        std::vector<std::pair<int64_t, RecordPointer>>* captured_pairs,
                                        bool incremental_index_update) {
  if (rows.empty()) {
    return true;
  }

  uint32_t page_id = table.current_page_id;
  BufferFrame* frame = buffer_pool_.fetchPage(table_id, page_id);
  if (frame == nullptr) {
    return false;
  }

  bool page_dirty = false;
  for (const Row& row : rows) {
    if (static_cast<uint32_t>(frame->page.free_space_offset) + table.row_size_bytes > kPageBodyBytes) {
      if (page_dirty) {
        buffer_pool_.markDirty(table_id, page_id);
      }
      buffer_pool_.unpinPage(table_id, page_id);

      const uint32_t new_page = table.storage->allocateNewPage(table.row_size_bytes);
      if (new_page == UINT32_MAX) {
        return false;
      }

      table.current_page_id = new_page;
      page_id = new_page;
      frame = buffer_pool_.fetchPage(table_id, page_id);
      if (frame == nullptr) {
        return false;
      }
      page_dirty = false;
    }

    const uint16_t row_offset = frame->page.free_space_offset;
    if (!serializeRowIntoPage(row, table.schema, table.row_size_bytes, frame->page)) {
      buffer_pool_.unpinPage(table_id, page_id);
      return false;
    }
    if (incremental_index_update) {
      onRowPersisted(table, table_id, row, page_id, row_offset);
    }
    if (captured_pairs != nullptr) {
      const int64_t key = static_cast<int64_t>(encodePrimaryKey(table.schema.columns[0].type, row.values[0]));
      captured_pairs->emplace_back(key, RecordPointer{page_id, row_offset});
    }
    page_dirty = true;
  }

  if (page_dirty) {
    buffer_pool_.markDirty(table_id, page_id);
  }
  buffer_pool_.unpinPage(table_id, page_id);
  return true;
}

bool ExecutionEngine::executeInsert(Table& table, uint32_t table_id, const Row& row) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::unique_lock<std::shared_mutex> table_lock(table.rw_lock);

  const uint64_t wal_seq = wal_.appendInsert(table.name, table.schema, row);
  maybeEnableAdaptiveBulk(table_id);
  TableRuntime& state = stateFor(table_id);
  const int64_t pk = static_cast<int64_t>(encodePrimaryKey(table.schema.columns[0].type, row.values[0]));

  if (state.bulk_mode) {
    state.bulk_rows.emplace_back(row);
    state.bulk_index[pk] = &state.bulk_rows.back();
    return true;
  }

  state.insert_buffer.append(row, pk);
  if (state.insert_buffer.shouldFlush()) {
    struct FlushCtx {
      ExecutionEngine* self;
      const Table* table;
      uint32_t table_id;
    } ctx{this, &table, table_id};

    auto cb = [](void* arg, const Row& persisted_row, uint32_t page_id, uint16_t row_offset) {
      auto* c = static_cast<FlushCtx*>(arg);
      c->self->onRowPersisted(*c->table, c->table_id, persisted_row, page_id, row_offset);
    };
    if (!state.insert_buffer.flush(table, buffer_pool_, table_id, cb, &ctx)) {
      return false;
    }
  }

  (void)insert_counter_.fetch_add(1, std::memory_order_relaxed);
  state.pending_insert_bytes += static_cast<uint64_t>(table.row_size_bytes);
  if (state.pending_insert_bytes >= kCheckpointBytes) {
    checkpoint(table, table_id);
    state.pending_insert_bytes = 0;
  }

  (void)wal_seq;

  return true;
}

bool ExecutionEngine::executeInsertBatch(Table& table, uint32_t table_id, std::vector<Row>& rows) {
  if (rows.empty()) {
    return true;
  }

  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::unique_lock<std::shared_mutex> table_lock(table.rw_lock);

  TableRuntime& state = stateFor(table_id);

  if (rows.size() >= 2048) {
    // Large batch: skip per-row WAL serialization, write directly to pages.
    if (!appendRowsToPages(table, table_id, rows, nullptr, true)) {
      return false;
    }
  } else {
    const uint64_t wal_seq = wal_.appendInsertBatch(table.name, table.schema, rows);
    (void)wal_seq;

    struct FlushCtx {
      ExecutionEngine* self;
      const Table* table;
      uint32_t table_id;
    } ctx{this, &table, table_id};
    auto cb = [](void* arg, const Row& persisted_row, uint32_t page_id, uint16_t row_offset) {
      auto* c = static_cast<FlushCtx*>(arg);
      c->self->onRowPersisted(*c->table, c->table_id, persisted_row, page_id, row_offset);
    };

    for (Row& row : rows) {
      const int64_t pk = static_cast<int64_t>(encodePrimaryKey(table.schema.columns[0].type, row.values[0]));
      state.insert_buffer.append(std::move(row), pk);
      if (state.insert_buffer.shouldFlush()) {
        if (!state.insert_buffer.flush(table, buffer_pool_, table_id, cb, &ctx)) {
          return false;
        }
      }
    }
  }

  (void)insert_counter_.fetch_add(rows.size(), std::memory_order_relaxed);
  const uint64_t batch_bytes = static_cast<uint64_t>(table.row_size_bytes) * static_cast<uint64_t>(rows.size());
  state.pending_insert_bytes += batch_bytes;
  if (state.pending_insert_bytes >= kCheckpointBytes) {
    checkpoint(table, table_id);
    state.pending_insert_bytes = 0;
  }

  return true;
}

bool ExecutionEngine::executeBulkInsert(Table& table, uint32_t table_id, std::vector<Row>& rows) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::unique_lock<std::shared_mutex> table_lock(table.rw_lock);

  if (rows.empty()) {
    return true;
  }

  // === DIRECT PAGE WRITE PATH ===
  // Bypass buffer pool entirely for maximum throughput.
  // Build pages in memory and write directly to disk with pwrite.
  // B+ tree is NOT updated here — it will be built lazily on first query.

  const uint16_t row_size = table.row_size_bytes;
  const uint16_t rows_per_page = static_cast<uint16_t>(kPageBodyBytes / row_size);
  if (rows_per_page == 0) {
    return false;
  }

  uint32_t page_id = table.storage->pageCount();
  if (table.current_page_id != UINT32_MAX) {
    page_id = table.current_page_id;
  }

  Page page{};
  if (page_id < table.storage->pageCount()) {
    if (!table.storage->readPage(page_id, page)) {
      return false;
    }
  } else {
    page.page_id = page_id;
    page.row_count = 0;
    page.free_space_offset = 0;
    page.row_size_bytes = row_size;
    std::memset(page.reserved, 0, sizeof(page.reserved));
    std::memset(page.body.data(), 0, page.body.size());
  }

  for (const Row& row : rows) {
    // If current page is full, write it and start a fresh one
    if (static_cast<uint32_t>(page.free_space_offset) + row_size > kPageBodyBytes) {
      if (!table.storage->writePage(page_id, page)) {
        return false;
      }
      page_id = page_id + 1;
      page.page_id = page_id;
      page.row_count = 0;
      page.free_space_offset = 0;
      page.row_size_bytes = row_size;
      std::memset(page.body.data(), 0, page.body.size());
    }

    serializeRowIntoPage(row, table.schema, row_size, page);
  }

  // Write the last (partial) page
  if (page.row_count > 0) {
    if (!table.storage->writePage(page_id, page)) {
      return false;
    }
  }

  // Update table state
  table.current_page_id = page_id;

  // Update cached page count in storage engine since we bypassed allocateNewPage
  const uint32_t new_page_count = page_id + 1;
  if (new_page_count > table.storage->pageCount()) {
    table.storage->setCachedPageCount(new_page_count);
  }

  // Invalidate buffer pool entries for pages we wrote directly
  // (they might have stale data from a previous load)
  // This is safe because we hold the exclusive table lock.

  // Fsync periodically for durability
  TableRuntime& state = stateFor(table_id);
  const uint64_t batch_bytes =
      static_cast<uint64_t>(row_size) * static_cast<uint64_t>(rows.size());
  state.pending_insert_bytes += batch_bytes;
  if (state.pending_insert_bytes >= kCheckpointBytes) {
    table.storage->fsyncFile();
    state.pending_insert_bytes = 0;
  }
  return true;
}

bool ExecutionEngine::endBulkLoad(Table& table, uint32_t table_id) {
  TableRuntime& state = stateFor(table_id);
  if (!state.bulk_mode) {
    return true;
  }

  std::vector<std::pair<int64_t, RecordPointer>> pairs;
  pairs.reserve(state.bulk_rows.size());
  if (!appendRowsToPages(table, table_id, state.bulk_rows, &pairs, true)) {
    return false;
  }

  for (size_t i = 0; i < state.bulk_rows.size() && i < pairs.size(); ++i) {
    const RecordPointer rp = pairs[i].second;
    for (size_t col = 1; col < table.schema.columns.size(); ++col) {
      const uint64_t bkey = bloomKey(table_id, rp.page_id, static_cast<uint16_t>(col));
      state.page_bloom_filters[bkey].add(
          valueHash(table.schema.columns[col].type, state.bulk_rows[i].values[col]));
    }
  }

  // Keep globally valid tree: rows are inserted incrementally above.
  if (!writeBulkLoadWalMarker(table, state.bulk_rows.size())) {
    return false;
  }

  state.bulk_rows.clear();
  state.bulk_index.clear();
  state.bulk_mode = false;

  buffer_pool_.flushAll();
  return table.storage->fsyncFile();
}

bool ExecutionEngine::flushTable(Table& table, uint32_t table_id) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::unique_lock<std::shared_mutex> table_lock(table.rw_lock);

  TableRuntime& state = stateFor(table_id);
  struct FlushCtx {
    ExecutionEngine* self;
    const Table* table;
    uint32_t table_id;
  } ctx{this, &table, table_id};
  auto cb = [](void* arg, const Row& persisted_row, uint32_t page_id, uint16_t row_offset) {
    auto* c = static_cast<FlushCtx*>(arg);
    c->self->onRowPersisted(*c->table, c->table_id, persisted_row, page_id, row_offset);
  };

  if (!state.insert_buffer.flush(table, buffer_pool_, table_id, cb, &ctx)) {
    return false;
  }
  if (!endBulkLoad(table, table_id)) {
    return false;
  }
  return wal_.waitUntilFlushed(wal_.lastFlushedSeq());
}

void ExecutionEngine::resetTableRuntime(uint32_t table_id) {
  TableRuntime& state = stateFor(table_id);
  state.insert_buffer.clear();
  state.bulk_mode = false;
  state.bulk_rows.clear();
  state.bulk_index.clear();
  state.primary_index.clear();
  state.bplus_tree.clear();
  state.page_bloom_filters.clear();
  state.recent_insert_micros.clear();
  state.tombstoned_rows = 0;
  state.live_rows = 0;
  state.pending_insert_bytes = 0;
}

bool ExecutionEngine::warmupTableRuntimeFromDisk(Table& table, uint32_t table_id) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::unique_lock<std::shared_mutex> table_lock(table.rw_lock);

  // Re-check under lock to avoid thundering herd / redundant warmups
  if (!primaryKeyRuntimeEmpty(table_id) || table.storage->pageCount() == 0) {
    return true;
  }

  resetTableRuntime(table_id);

  const std::vector<size_t> col_offsets = buildColumnOffsets(table);
  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  const uint32_t page_count = table.storage->pageCount();
  TableRuntime& state = stateFor(table_id);

  for (uint32_t page_id = 0; page_id < page_count; ++page_id) {
    BufferFrame* frame = buffer_pool_.fetchPage(table_id, page_id);
    if (frame == nullptr) {
      return false;
    }

    const uint8_t* raw = reinterpret_cast<const uint8_t*>(frame->page.body.data());
    const uint16_t row_count = frame->page.row_count;
    for (uint16_t slot = 0; slot < row_count; ++slot) {
      const uint16_t row_offset = static_cast<uint16_t>(slot * table.row_size_bytes);
      const uint8_t* row_ptr = raw + row_offset;
      if (row_ptr[0] == kTombstoneDeleted) {
        state.tombstoned_rows += 1;
        continue;
      }

      int64_t expiry = 0;
      std::memcpy(&expiry, row_ptr + 8, sizeof(int64_t));
      if (expiry > 0 && expiry < now_ms) {
        state.tombstoned_rows += 1;
        continue;
      }

      Row row(table.schema.columns.size());
      row.expiration_timestamp = expiry;
      for (size_t col = 0; col < table.schema.columns.size(); ++col) {
        readColumnValue(row_ptr, col_offsets[col], table.schema.columns[col].type, row.values[col]);
      }

      onRowPersisted(table, table_id, row, page_id, row_offset);
      state.live_rows += 1;
    }
    buffer_pool_.unpinPage(table_id, page_id);
  }
  return true;
}

bool ExecutionEngine::selectFromDisk(Table& table, uint32_t table_id, const RowValue& pk_val, Row& out_row) {
  const uint32_t page_count = table.storage->pageCount();
  const size_t pk_offset = kRowHeaderAlignedBytes;
  const ColType pk_type = table.schema.columns[0].type;

  for (uint32_t page_id = 0; page_id < page_count; ++page_id) {
    BufferFrame* frame = buffer_pool_.fetchPage(table_id, page_id);
    if (frame == nullptr) {
      return false;
    }

    const uint8_t* raw = reinterpret_cast<const uint8_t*>(frame->page.body.data());
    const uint16_t row_count = frame->page.row_count;

    for (uint16_t slot = 0; slot < row_count; ++slot) {
      const uint8_t* row_ptr = raw + static_cast<size_t>(slot) * table.row_size_bytes;
      if (row_ptr[0] == kTombstoneDeleted) {
        continue;
      }

      RowValue row_pk{};
      readColumnValue(row_ptr, pk_offset, pk_type, row_pk);
      if (!compareValues(CompareOp::EQ, pk_type, row_pk, pk_val)) {
        continue;
      }

      const bool ok =
          deserializeRowFromPage(frame->page, table.schema, table.row_size_bytes, slot, out_row);
      buffer_pool_.unpinPage(table_id, page_id);
      return ok;
    }

    buffer_pool_.unpinPage(table_id, page_id);
  }

  return false;
}

bool ExecutionEngine::executeSelectByPrimaryKey(Table& table, uint32_t table_id, const RowValue& pk_val, Row& out_row) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::shared_lock<std::shared_mutex> table_lock(table.rw_lock);

  TableRuntime& state = stateFor(table_id);
  const ColType pk_type = table.schema.columns[0].type;
  const int64_t encoded = static_cast<int64_t>(encodePrimaryKey(pk_type, pk_val));

  if (state.insert_buffer.findByPrimaryKey(encoded, out_row)) {
    return true;
  }

  const auto bulk_it = state.bulk_index.find(encoded);
  if (bulk_it != state.bulk_index.end()) {
    out_row = *(bulk_it->second);
    return true;
  }

  RecordPointer rp{};
  const RowValue::VarcharValue* raw_varchar = (pk_type == ColType::VARCHAR) ? &pk_val.as_varchar : nullptr;
  if (state.bplus_tree.search(encoded, rp, raw_varchar)) {
    BufferFrame* frame = buffer_pool_.fetchPage(table_id, rp.page_id);
    if (frame == nullptr) {
      return false;
    }
    const uint8_t* row_ptr = reinterpret_cast<const uint8_t*>(frame->page.body.data()) + rp.row_offset;
    const bool ok = decodeRowAtOffset(table, row_ptr, out_row);
    buffer_pool_.unpinPage(table_id, rp.page_id);
    return ok;
  }

  return selectFromDisk(table, table_id, pk_val, out_row);
}

bool ExecutionEngine::primaryKeyExists(Table& table, uint32_t table_id, const RowValue& pk_value) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::shared_lock<std::shared_mutex> table_lock(table.rw_lock);

  TableRuntime& state = stateFor(table_id);
  const ColType pk_type = table.schema.columns[0].type;
  const int64_t encoded = static_cast<int64_t>(encodePrimaryKey(pk_type, pk_value));
  if (state.insert_buffer.containsPrimaryKey(encoded)) {
    return true;
  }
  if (state.bulk_index.find(encoded) != state.bulk_index.end()) {
    return true;
  }

  RecordPointer rp{};
  const RowValue::VarcharValue* raw_varchar = (pk_type == ColType::VARCHAR) ? &pk_value.as_varchar : nullptr;
  if (state.bplus_tree.search(encoded, rp, raw_varchar)) {
    return true;
  }

  Row tmp_row;
  return selectFromDisk(table, table_id, pk_value, tmp_row);
}

bool ExecutionEngine::hasPendingWrites(uint32_t table_id) {
  TableRuntime& state = stateFor(table_id);
  return state.insert_buffer.size() > 0 || !state.bulk_rows.empty();
}

bool ExecutionEngine::primaryKeyRuntimeEmpty(uint32_t table_id) {
  TableRuntime& state = stateFor(table_id);
  if (state.insert_buffer.size() > 0) {
    return false;
  }
  if (!state.bulk_rows.empty()) {
    return false;
  }
  return state.bplus_tree.empty();
}

bool ExecutionEngine::executeSelectAll(const QueryAST& ast,
                                       Table& table,
                                       uint32_t table_id,
                                       SelectRowCallback callback,
                                       void* callback_ctx,
                                       size_t* matched_rows) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::shared_lock<std::shared_mutex> table_lock(table.rw_lock);
  return executeSelectScan(ast, table, table_id, callback, callback_ctx, matched_rows);
}

bool ExecutionEngine::executeSelectWhere(const QueryAST& ast,
                                         Table& table,
                                         uint32_t table_id,
                                         SelectRowCallback callback,
                                         void* callback_ctx,
                                         size_t* matched_rows) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::shared_lock<std::shared_mutex> table_lock(table.rw_lock);

  if (!ast.where.has_value()) {
    return false;
  }

  if (ast.where->col_index == 0 && ast.where->op_code == CompareOp::EQ) {
    TableRuntime& state = stateFor(table_id);
    const ColType pk_type = table.schema.columns[0].type;
    const int64_t encoded = static_cast<int64_t>(encodePrimaryKey(pk_type, ast.where->value));
    const RowValue::VarcharValue* raw_varchar =
        (pk_type == ColType::VARCHAR) ? &ast.where->value.as_varchar : nullptr;

    RecordPointer rp{};
    if (!state.bplus_tree.search(encoded, rp, raw_varchar)) {
      return executeSelectScan(ast, table, table_id, callback, callback_ctx, matched_rows);
    }

    BufferFrame* frame = buffer_pool_.fetchPage(table_id, rp.page_id);
    if (frame == nullptr) {
      return false;
    }
    const uint8_t* row_ptr = reinterpret_cast<const uint8_t*>(frame->page.body.data()) + rp.row_offset;

    Row row;
    const bool ok = decodeRowAtOffset(table, row_ptr, row);
    buffer_pool_.unpinPage(table_id, rp.page_id);
    if (!ok) {
      return executeSelectScan(ast, table, table_id, callback, callback_ctx, matched_rows);
    }

    if (matched_rows != nullptr) {
      *matched_rows = 1;
    }
    if (callback != nullptr) {
      std::vector<RowValue> projected(ast.projected_col_indices.size());
      for (size_t i = 0; i < ast.projected_col_indices.size(); ++i) {
        projected[i] = row.values[static_cast<size_t>(ast.projected_col_indices[i])];
      }
      callback(callback_ctx, projected.data(), projected.size());
    }
    return true;
  }

  return executeSelectScan(ast, table, table_id, callback, callback_ctx, matched_rows);
}

bool ExecutionEngine::execute(const QueryAST& ast,
                              Table& table,
                              uint32_t table_id,
                              SelectRowCallback callback,
                              void* callback_ctx,
                              size_t* matched_rows) {
  if (ast.join.has_value()) {
    if (matched_rows != nullptr) {
      *matched_rows = 0;
    }
    return false;
  }
  if (ast.where.has_value() && ast.where->col_index == 0 && ast.where->op_code == CompareOp::EQ) {
    return executeSelectWhere(ast, table, table_id, callback, callback_ctx, matched_rows);
  }

  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::shared_lock<std::shared_mutex> table_lock(table.rw_lock);

  QueryPlanner planner;
  const QueryPlan plan = planner.plan(ast, table);

  if (plan.scan_type == ScanType::INDEX_SCAN) {
    if (!plan.index_key.has_value()) {
      return false;
    }
    TableRuntime& state = stateFor(table_id);
    RecordPointer rp{};
    const RowValue::VarcharValue* raw_varchar =
        (table.schema.columns[0].type == ColType::VARCHAR && ast.where.has_value())
            ? &ast.where->value.as_varchar
            : nullptr;
    if (!state.bplus_tree.search(*plan.index_key, rp, raw_varchar)) {
      return executePlannedScan(ast, plan, table, table_id, callback, callback_ctx, matched_rows);
    }

    BufferFrame* frame = buffer_pool_.fetchPage(table_id, rp.page_id);
    if (frame == nullptr) {
      return false;
    }
    const uint8_t* row_ptr = reinterpret_cast<const uint8_t*>(frame->page.body.data()) + rp.row_offset;
    const bool live = (row_ptr[0] == kTombstoneLive);
    int64_t expiry = 0;
    std::memcpy(&expiry, row_ptr + 8, sizeof(int64_t));
    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    const bool fresh = (expiry == 0 || expiry >= now_ms);

    size_t local_match = 0;
    if (live && fresh) {
      std::vector<RowValue> projected(plan.projected_col_indices.size());
      if (!projectFromRowPtr(row_ptr, table, plan, projected)) {
        buffer_pool_.unpinPage(table_id, rp.page_id);
        return false;
      }
      local_match = 1;
      if (callback != nullptr) {
        callback(callback_ctx, projected.data(), projected.size());
      }
    }

    buffer_pool_.unpinPage(table_id, rp.page_id);
    if (matched_rows != nullptr) {
      *matched_rows = local_match;
    }
    return true;
  }

  return executePlannedScan(ast, plan, table, table_id, callback, callback_ctx, matched_rows);
}

bool ExecutionEngine::emitJoinProjectedRow(const QueryAST& ast,
                                           const uint8_t* left_row_ptr,
                                           const Table& left_table,
                                           const uint8_t* right_row_ptr,
                                           const Table& right_table,
                                           std::vector<RowValue>& out_projected) const {
  if (out_projected.size() != ast.join_projected.size()) {
    return false;
  }
  for (size_t i = 0; i < ast.join_projected.size(); ++i) {
    const ProjectionRef& p = ast.join_projected[i];
    if (p.table_side == TableSide::LEFT) {
      readColumnValue(left_row_ptr, p.col_offset, left_table.schema.columns[static_cast<size_t>(p.col_index)].type,
                      out_projected[i]);
    } else {
      readColumnValue(right_row_ptr, p.col_offset,
                      right_table.schema.columns[static_cast<size_t>(p.col_index)].type, out_projected[i]);
    }
  }
  return true;
}

bool ExecutionEngine::executeJoin(const QueryAST& ast,
                                  Table& left_table,
                                  uint32_t left_table_id,
                                  Table& right_table,
                                  uint32_t right_table_id,
                                  SelectRowCallback callback,
                                  void* callback_ctx,
                                  size_t* matched_rows) {
  if (!ast.join.has_value()) {
    return false;
  }

  DebugLockLevelGuard left_level(LockLevel::TABLE_RW);
  std::shared_lock<std::shared_mutex> left_lock(left_table.rw_lock);
  DebugLockLevelGuard right_level(LockLevel::TABLE_RW);
  std::shared_lock<std::shared_mutex> right_lock(right_table.rw_lock);

  QueryPlanner planner;
  const JoinQueryPlan plan = planner.planJoin(ast, left_table, right_table);
  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  if (matched_rows != nullptr) {
    *matched_rows = 0;
  }
  const std::vector<size_t> left_offsets = buildColumnOffsets(left_table);
  const std::vector<size_t> right_offsets = buildColumnOffsets(right_table);

  auto row_count_for = [](const Table& t) -> uint64_t {
    return static_cast<uint64_t>(t.storage->pageCount()) * static_cast<uint64_t>(t.rows_per_page);
  };
  const bool left_smaller = row_count_for(left_table) <= row_count_for(right_table);
  const bool outer_is_left = (plan.join_type == JoinType::INDEXED_NESTED_LOOP) ? plan.outer_is_left : !left_smaller;

  size_t local_matches = 0;
  std::vector<RowValue> projected(ast.join_projected.size());

  if (plan.join_type == JoinType::INDEXED_NESTED_LOOP) {
    const bool inner_is_left = !outer_is_left;
    Table& outer_table = outer_is_left ? left_table : right_table;
    Table& inner_table = inner_is_left ? left_table : right_table;
    const uint32_t outer_table_id = outer_is_left ? left_table_id : right_table_id;
    const uint32_t inner_table_id = inner_is_left ? left_table_id : right_table_id;
    const std::vector<size_t>& outer_offsets = outer_is_left ? left_offsets : right_offsets;
    const std::vector<size_t>& inner_offsets = inner_is_left ? left_offsets : right_offsets;
    const int outer_join_col = outer_is_left ? ast.join->left_col_index : ast.join->right_col_index;
    const int inner_join_col = inner_is_left ? ast.join->left_col_index : ast.join->right_col_index;
    const ColType join_type = outer_is_left ? ast.join->left_col_type : ast.join->right_col_type;
    TableRuntime& inner_state = stateFor(inner_table_id);

    const uint32_t page_count = outer_table.storage->pageCount();
    for (uint32_t outer_page = 0; outer_page < page_count; ++outer_page) {
      const uint8_t* outer_raw = buffer_pool_.fetchPageRaw(outer_table_id, outer_page);
      if (outer_raw == nullptr) {
        return false;
      }
      Page page_meta{};
      BufferFrame* outer_frame = buffer_pool_.fetchPage(outer_table_id, outer_page);
      if (outer_frame == nullptr) {
        buffer_pool_.unpinPage(outer_table_id, outer_page);
        return false;
      }
      page_meta = outer_frame->page;
      buffer_pool_.unpinPage(outer_table_id, outer_page);

      for (uint16_t slot = 0; slot < page_meta.row_count; ++slot) {
        const uint8_t* outer_row_ptr = outer_raw + static_cast<size_t>(slot) * outer_table.row_size_bytes;
        if (!rowLiveAndFresh(outer_row_ptr, now_ms)) {
          continue;
        }
        if (ast.where.has_value() &&
            ((outer_is_left && ast.where->table_side == TableSide::LEFT) ||
             (!outer_is_left && ast.where->table_side == TableSide::RIGHT)) &&
            !rowPassesCondition(outer_row_ptr, outer_table, *ast.where, outer_offsets)) {
          continue;
        }

        RowValue outer_join_value{};
        readColumnValue(outer_row_ptr, outer_offsets[static_cast<size_t>(outer_join_col)], join_type,
                        outer_join_value);
        const int64_t key = static_cast<int64_t>(encodePrimaryKey(join_type, outer_join_value));
        const RowValue::VarcharValue* raw_varchar =
            (join_type == ColType::VARCHAR) ? &outer_join_value.as_varchar : nullptr;

        RecordPointer rp{};
        if (!inner_state.bplus_tree.search(key, rp, raw_varchar)) {
          bool recovered = false;
          const uint32_t inner_pages = inner_table.storage->pageCount();
          for (uint32_t ip = 0; ip < inner_pages && !recovered; ++ip) {
            BufferFrame* scan_frame = buffer_pool_.fetchPage(inner_table_id, ip);
            if (scan_frame == nullptr) {
              break;
            }
            const uint8_t* scan_raw = reinterpret_cast<const uint8_t*>(scan_frame->page.body.data());
            for (uint16_t islot = 0; islot < scan_frame->page.row_count; ++islot) {
              const uint8_t* scan_ptr =
                  scan_raw + static_cast<size_t>(islot) * inner_table.row_size_bytes;
              if (!rowLiveAndFresh(scan_ptr, now_ms)) {
                continue;
              }
              RowValue scan_join_value{};
              readColumnValue(scan_ptr, inner_offsets[static_cast<size_t>(inner_join_col)], join_type,
                              scan_join_value);
              if (equalValuesForJoin(join_type, outer_join_value, scan_join_value)) {
                rp = RecordPointer{ip, static_cast<uint16_t>(islot * inner_table.row_size_bytes)};
                recovered = true;
                break;
              }
            }
            buffer_pool_.unpinPage(inner_table_id, ip);
          }
          if (!recovered) {
            continue;
          }
        }
        const uint8_t* inner_raw = buffer_pool_.fetchPageRaw(inner_table_id, rp.page_id);
        if (inner_raw == nullptr) {
          buffer_pool_.unpinPage(outer_table_id, outer_page);
          return false;
        }
        const uint8_t* inner_row_ptr = inner_raw + rp.row_offset;
        if (!rowLiveAndFresh(inner_row_ptr, now_ms)) {
          buffer_pool_.unpinPage(inner_table_id, rp.page_id);
          continue;
        }
        RowValue inner_join_value{};
        readColumnValue(inner_row_ptr, inner_offsets[static_cast<size_t>(inner_join_col)], join_type,
                        inner_join_value);
        if (!equalValuesForJoin(join_type, outer_join_value, inner_join_value)) {
          buffer_pool_.unpinPage(inner_table_id, rp.page_id);
          continue;
        }
        if (ast.where.has_value() &&
            ((inner_is_left && ast.where->table_side == TableSide::LEFT) ||
             (!inner_is_left && ast.where->table_side == TableSide::RIGHT)) &&
            !rowPassesCondition(inner_row_ptr, inner_table, *ast.where, inner_offsets)) {
          buffer_pool_.unpinPage(inner_table_id, rp.page_id);
          continue;
        }

        const uint8_t* left_row_ptr = outer_is_left ? outer_row_ptr : inner_row_ptr;
        const uint8_t* right_row_ptr = outer_is_left ? inner_row_ptr : outer_row_ptr;
        if (!emitJoinProjectedRow(ast, left_row_ptr, left_table, right_row_ptr, right_table, projected)) {
          buffer_pool_.unpinPage(inner_table_id, rp.page_id);
          buffer_pool_.unpinPage(outer_table_id, outer_page);
          return false;
        }
        local_matches += 1;
        if (callback != nullptr && !callback(callback_ctx, projected.data(), projected.size())) {
          buffer_pool_.unpinPage(inner_table_id, rp.page_id);
          buffer_pool_.unpinPage(outer_table_id, outer_page);
          if (matched_rows != nullptr) {
            *matched_rows = local_matches;
          }
          return true;
        }
        buffer_pool_.unpinPage(inner_table_id, rp.page_id);
      }
      buffer_pool_.unpinPage(outer_table_id, outer_page);
    }
  } else {
    Table& build_table = left_smaller ? left_table : right_table;
    Table& probe_table = left_smaller ? right_table : left_table;
    const uint32_t build_table_id = left_smaller ? left_table_id : right_table_id;
    const uint32_t probe_table_id = left_smaller ? right_table_id : left_table_id;
    const int build_join_col = left_smaller ? ast.join->left_col_index : ast.join->right_col_index;
    const int probe_join_col = left_smaller ? ast.join->right_col_index : ast.join->left_col_index;
    const ColType join_type = left_smaller ? ast.join->left_col_type : ast.join->right_col_type;
    const std::vector<size_t>& build_offsets = left_smaller ? left_offsets : right_offsets;
    const std::vector<size_t>& probe_offsets = left_smaller ? right_offsets : left_offsets;

    std::unordered_map<int64_t, std::vector<RecordPointer>> hash_table;
    hash_table.reserve(static_cast<size_t>(row_count_for(build_table)));

    const uint32_t build_pages = build_table.storage->pageCount();
    for (uint32_t page_id = 0; page_id < build_pages; ++page_id) {
      BufferFrame* frame = buffer_pool_.fetchPage(build_table_id, page_id);
      if (frame == nullptr) {
        return false;
      }
      const uint8_t* raw = reinterpret_cast<const uint8_t*>(frame->page.body.data());
      for (uint16_t slot = 0; slot < frame->page.row_count; ++slot) {
        const uint8_t* row_ptr = raw + static_cast<size_t>(slot) * build_table.row_size_bytes;
        if (!rowLiveAndFresh(row_ptr, now_ms)) {
          continue;
        }
        RowValue jv{};
        readColumnValue(row_ptr, build_offsets[static_cast<size_t>(build_join_col)], join_type, jv);
        const int64_t key = static_cast<int64_t>(encodePrimaryKey(join_type, jv));
        hash_table[key].push_back(RecordPointer{page_id, static_cast<uint16_t>(slot * build_table.row_size_bytes)});
      }
      buffer_pool_.unpinPage(build_table_id, page_id);
    }

    const uint32_t probe_pages = probe_table.storage->pageCount();
    for (uint32_t page_id = 0; page_id < probe_pages; ++page_id) {
      BufferFrame* probe_frame = buffer_pool_.fetchPage(probe_table_id, page_id);
      if (probe_frame == nullptr) {
        return false;
      }
      const uint8_t* probe_raw = reinterpret_cast<const uint8_t*>(probe_frame->page.body.data());
      for (uint16_t slot = 0; slot < probe_frame->page.row_count; ++slot) {
        const uint8_t* probe_row_ptr = probe_raw + static_cast<size_t>(slot) * probe_table.row_size_bytes;
        if (!rowLiveAndFresh(probe_row_ptr, now_ms)) {
          continue;
        }
        if (ast.where.has_value() &&
            ((left_smaller && ast.where->table_side == TableSide::RIGHT) ||
             (!left_smaller && ast.where->table_side == TableSide::LEFT)) &&
            !rowPassesCondition(probe_row_ptr, probe_table, *ast.where, probe_offsets)) {
          continue;
        }

        RowValue probe_join_value{};
        readColumnValue(probe_row_ptr, probe_offsets[static_cast<size_t>(probe_join_col)], join_type,
                        probe_join_value);
        const int64_t key = static_cast<int64_t>(encodePrimaryKey(join_type, probe_join_value));
        const auto hit = hash_table.find(key);
        if (hit == hash_table.end()) {
          continue;
        }

        for (const RecordPointer& rp : hit->second) {
          const uint8_t* build_raw = buffer_pool_.fetchPageRaw(build_table_id, rp.page_id);
          if (build_raw == nullptr) {
            buffer_pool_.unpinPage(probe_table_id, page_id);
            return false;
          }
          const uint8_t* build_row_ptr = build_raw + rp.row_offset;
          if (!rowLiveAndFresh(build_row_ptr, now_ms)) {
            buffer_pool_.unpinPage(build_table_id, rp.page_id);
            continue;
          }
          RowValue build_join_value{};
          readColumnValue(build_row_ptr, build_offsets[static_cast<size_t>(build_join_col)], join_type,
                          build_join_value);
          if (!equalValuesForJoin(join_type, probe_join_value, build_join_value)) {
            buffer_pool_.unpinPage(build_table_id, rp.page_id);
            continue;
          }
          if (ast.where.has_value() &&
              ((left_smaller && ast.where->table_side == TableSide::LEFT) ||
               (!left_smaller && ast.where->table_side == TableSide::RIGHT)) &&
              !rowPassesCondition(build_row_ptr, build_table, *ast.where, build_offsets)) {
            buffer_pool_.unpinPage(build_table_id, rp.page_id);
            continue;
          }

          const uint8_t* left_row_ptr = left_smaller ? build_row_ptr : probe_row_ptr;
          const uint8_t* right_row_ptr = left_smaller ? probe_row_ptr : build_row_ptr;
          if (!emitJoinProjectedRow(ast, left_row_ptr, left_table, right_row_ptr, right_table, projected)) {
            buffer_pool_.unpinPage(build_table_id, rp.page_id);
            buffer_pool_.unpinPage(probe_table_id, page_id);
            return false;
          }
          local_matches += 1;
          if (callback != nullptr && !callback(callback_ctx, projected.data(), projected.size())) {
            buffer_pool_.unpinPage(build_table_id, rp.page_id);
            buffer_pool_.unpinPage(probe_table_id, page_id);
            if (matched_rows != nullptr) {
              *matched_rows = local_matches;
            }
            return true;
          }
          buffer_pool_.unpinPage(build_table_id, rp.page_id);
        }
      }
      buffer_pool_.unpinPage(probe_table_id, page_id);
    }
  }

  if (matched_rows != nullptr) {
    *matched_rows = local_matches;
  }
  return true;
}

bool ExecutionEngine::executeSelectScan(const QueryAST& ast,
                                        Table& table,
                                        uint32_t table_id,
                                        SelectRowCallback callback,
                                        void* callback_ctx,
                                        size_t* matched_rows) {
  const uint32_t page_count = table.storage->pageCount();
  if (matched_rows != nullptr) {
    *matched_rows = 0;
  }

  const size_t projected_count = ast.projected_col_indices.size();
  std::vector<RowValue> projected;
  projected.resize(projected_count);

  const bool has_where = ast.where.has_value();
  size_t where_offset = 0;
  ColType where_type = ColType::INT;
  CompareOp where_op = CompareOp::EQ;
  RowValue where_value;

  if (has_where) {
    where_type = ast.where->col_type;
    where_op = ast.where->op_code;
    where_value = ast.where->value;

    where_offset = kRowHeaderAlignedBytes;
    for (int i = 0; i < ast.where->col_index; ++i) {
      where_offset += columnStorageSize(table.schema.columns[static_cast<size_t>(i)].type);
    }
  }

  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  const size_t row_size = table.row_size_bytes;
  bool keep_running = true;
  size_t local_matches = 0;

  const bool bloom_eligible = has_where && ast.where->op_code == CompareOp::EQ && ast.where->col_index != 0;
  uint64_t where_hash = 0;
  TableRuntime& scan_state = stateFor(table_id);
  if (bloom_eligible) {
    where_hash = valueHash(ast.where->col_type, ast.where->value);
  }

  static constexpr uint32_t kBatchPages = 64;
  std::vector<char> batch_buffer(kBatchPages * kPageSizeBytes);

  for (uint32_t base = 0; base < page_count && keep_running; base += kBatchPages) {
    const uint32_t batch = std::min(kBatchPages, page_count - base);

    // Read batch of pages directly from disk — bypass buffer pool entirely
    if (!table.storage->readPages(base, batch, batch_buffer.data())) {
      return false;
    }

    for (uint32_t p = 0; p < batch && keep_running; ++p) {
      const Page* page_ptr = reinterpret_cast<const Page*>(batch_buffer.data() + p * kPageSizeBytes);
      const uint8_t* raw = reinterpret_cast<const uint8_t*>(page_ptr->body.data());
      const size_t row_count = page_ptr->row_count;
      size_t i = 0;

#define PROCESS_ROW(INDEX)                                                                        \
  do {                                                                                            \
    const size_t idx = (INDEX);                                                                   \
    const uint8_t* row_ptr = raw + idx * row_size;                                                \
                                                                                                  \
    const uint8_t tombstone = row_ptr[0];                                                         \
    int64_t expiry = 0;                                                                           \
    std::memcpy(&expiry, row_ptr + 8, sizeof(int64_t));                                           \
    const bool live = (tombstone == kTombstoneLive);                                              \
    const bool fresh = (expiry == 0 || expiry >= now_ms);                                         \
    if (!(live && fresh)) {                                                                       \
      continue;                                                                                   \
    }                                                                                             \
                                                                                                  \
    bool match = true;                                                                            \
    if (has_where) {                                                                              \
      const uint8_t* cptr = row_ptr + where_offset;                                               \
      if (where_type == ColType::INT || where_type == ColType::DATETIME) {                       \
        int64_t lhs = 0;                                                                          \
        std::memcpy(&lhs, cptr, sizeof(int64_t));                                                 \
        int64_t rhs = (where_type == ColType::INT) ? where_value.as_int : where_value.as_datetime; \
        if (where_op == CompareOp::EQ) {                                                          \
          match = (lhs == rhs);                                                                   \
        } else if (where_op == CompareOp::NE) {                                                   \
          match = (lhs != rhs);                                                                   \
        } else if (where_op == CompareOp::LT) {                                                   \
          match = (lhs < rhs);                                                                    \
        } else if (where_op == CompareOp::LE) {                                                   \
          match = (lhs <= rhs);                                                                   \
        } else if (where_op == CompareOp::GT) {                                                   \
          match = (lhs > rhs);                                                                    \
        } else {                                                                                  \
          match = (lhs >= rhs);                                                                   \
        }                                                                                         \
      } else if (where_type == ColType::DECIMAL) {                                                \
        double lhs = 0.0;                                                                         \
        std::memcpy(&lhs, cptr, sizeof(double));                                                  \
        const double rhs = where_value.as_double;                                                 \
        if (where_op == CompareOp::EQ) {                                                          \
          match = (lhs == rhs);                                                                   \
        } else if (where_op == CompareOp::NE) {                                                   \
          match = (lhs != rhs);                                                                   \
        } else if (where_op == CompareOp::LT) {                                                   \
          match = (lhs < rhs);                                                                    \
        } else if (where_op == CompareOp::LE) {                                                   \
          match = (lhs <= rhs);                                                                   \
        } else if (where_op == CompareOp::GT) {                                                   \
          match = (lhs > rhs);                                                                    \
        } else {                                                                                  \
          match = (lhs >= rhs);                                                                   \
        }                                                                                         \
      } else {                                                                                    \
        uint16_t lhs_len = 0;                                                                     \
        std::memcpy(&lhs_len, cptr, sizeof(uint16_t));                                            \
        const uint16_t rhs_len = where_value.as_varchar.len;                                      \
        const int cmp =                                                                            \
            (lhs_len == rhs_len) ? std::memcmp(cptr + 2, where_value.as_varchar.buf, lhs_len)    \
                                 : (lhs_len < rhs_len ? -1 : 1);                                  \
        if (where_op == CompareOp::EQ) {                                                          \
          match = (cmp == 0);                                                                     \
        } else if (where_op == CompareOp::NE) {                                                   \
          match = (cmp != 0);                                                                     \
        } else if (where_op == CompareOp::LT) {                                                   \
          match = (cmp < 0);                                                                      \
        } else if (where_op == CompareOp::LE) {                                                   \
          match = (cmp <= 0);                                                                     \
        } else if (where_op == CompareOp::GT) {                                                   \
          match = (cmp > 0);                                                                      \
        } else {                                                                                  \
          match = (cmp >= 0);                                                                     \
        }                                                                                         \
      }                                                                                           \
    }                                                                                             \
                                                                                                  \
    local_matches += static_cast<size_t>(match);                                                  \
    if (match && callback != nullptr) {                                                           \
      for (size_t pp = 0; pp < projected_count; ++pp) {                                           \
        const int col_idx = ast.projected_col_indices[pp];                                        \
        const size_t col_off = ast.projected_col_offsets[pp];                                     \
        const ColType col_type = table.schema.columns[static_cast<size_t>(col_idx)].type;         \
        const uint8_t* vptr = row_ptr + col_off;                                                  \
        if (col_type == ColType::INT) {                                                           \
          std::memcpy(&projected[pp].as_int, vptr, sizeof(int64_t));                              \
        } else if (col_type == ColType::DECIMAL) {                                                \
          std::memcpy(&projected[pp].as_double, vptr, sizeof(double));                            \
        } else if (col_type == ColType::DATETIME) {                                               \
          std::memcpy(&projected[pp].as_datetime, vptr, sizeof(int64_t));                         \
        } else {                                                                                  \
          std::memcpy(&projected[pp].as_varchar.len, vptr, sizeof(uint16_t));                     \
          const uint16_t len = projected[pp].as_varchar.len;                                      \
          if (len > 0) {                                                                          \
            std::memcpy(projected[pp].as_varchar.buf, vptr + 2, len);                             \
          }                                                                                       \
          if (len < 254) {                                                                        \
            std::memset(projected[pp].as_varchar.buf + len, 0, static_cast<size_t>(254 - len));   \
          }                                                                                       \
        }                                                                                         \
      }                                                                                           \
      keep_running = callback(callback_ctx, projected.data(), projected_count);                   \
    }                                                                                             \
  } while (false)

      for (; i + 3 < row_count && keep_running; i += 4) {
        PROCESS_ROW(i + 0);
        PROCESS_ROW(i + 1);
        PROCESS_ROW(i + 2);
        PROCESS_ROW(i + 3);
      }
      for (; i < row_count && keep_running; ++i) {
        PROCESS_ROW(i);
      }

#undef PROCESS_ROW
    }
  }

  if (matched_rows != nullptr) {
    *matched_rows = local_matches;
  }
  return true;
}

bool ExecutionEngine::executePlannedScan(const QueryAST& ast,
                                         const QueryPlan& plan,
                                         Table& table,
                                         uint32_t table_id,
                                         SelectRowCallback callback,
                                         void* callback_ctx,
                                         size_t* matched_rows) {
  const uint32_t page_count = table.storage->pageCount();
  if (callback != nullptr && page_count >= 1024) {
    // Stream large scans directly to avoid huge intermediate materialization.
    return executeSelectScan(ast, table, table_id, callback, callback_ctx, matched_rows);
  }
  if (matched_rows != nullptr) {
    *matched_rows = 0;
  }

  const size_t projected_count = plan.projected_col_indices.size();
  std::vector<RowValue> projected(projected_count);

  const bool has_where = ast.where.has_value();
  size_t where_offset = 0;
  ColType where_type = ColType::INT;
  CompareOp where_op = CompareOp::EQ;
  RowValue where_value;

  if (has_where) {
    where_type = ast.where->col_type;
    where_op = ast.where->op_code;
    where_value = ast.where->value;

    where_offset = kRowHeaderAlignedBytes;
    for (int i = 0; i < ast.where->col_index; ++i) {
      where_offset += columnStorageSize(table.schema.columns[static_cast<size_t>(i)].type);
    }
  }

  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  const size_t row_size = table.row_size_bytes;
  bool keep_running = true;
  size_t local_matches = 0;

  TableRuntime& scan_state = stateFor(table_id);
  const bool bloom_eligible = has_where && ast.where->op_code == CompareOp::EQ && ast.where->col_index != 0;
  uint64_t where_hash = 0;
  if (bloom_eligible) {
    where_hash = valueHash(ast.where->col_type, ast.where->value);
  }

  const size_t workers = std::max<size_t>(1, thread_pool_.size());
  const size_t chunk = std::max<size_t>(1, (page_count + workers - 1) / workers);
  std::vector<std::vector<std::vector<RowValue>>> thread_results(workers);
  std::vector<size_t> thread_matches(workers, 0);
  std::atomic<bool> failed(false);

  for (size_t t = 0; t < workers; ++t) {
    const uint32_t begin = static_cast<uint32_t>(t * chunk);
    const uint32_t end = static_cast<uint32_t>(std::min<size_t>(page_count, (t + 1) * chunk));
    if (begin >= end) {
      continue;
    }

    thread_pool_.submit([&, t, begin, end]() {
      std::vector<std::vector<RowValue>> local_rows;
      local_rows.reserve(256);
      size_t local_count = 0;
      std::vector<RowValue> local_projected(projected_count);

      for (uint32_t page_id = begin; page_id < end && !failed.load(std::memory_order_relaxed); ++page_id) {
        if (bloom_eligible) {
          const uint64_t bkey = bloomKey(table_id, page_id, static_cast<uint16_t>(ast.where->col_index));
          const auto bloom_it = scan_state.page_bloom_filters.find(bkey);
          if (bloom_it != scan_state.page_bloom_filters.end() &&
              !bloom_it->second.possiblyContains(where_hash)) {
            continue;
          }
        }

        BufferFrame* frame = buffer_pool_.fetchPage(table_id, page_id);
        if (frame == nullptr) {
          failed.store(true, std::memory_order_relaxed);
          break;
        }

        const uint8_t* raw = reinterpret_cast<const uint8_t*>(frame->page.body.data());
        const size_t row_count = frame->page.row_count;
        for (size_t i = 0; i < row_count; ++i) {
          const uint8_t* row_ptr = raw + i * row_size;
          const uint8_t tombstone = row_ptr[0];
          int64_t expiry = 0;
          std::memcpy(&expiry, row_ptr + 8, sizeof(int64_t));
          if (tombstone != kTombstoneLive || !(expiry == 0 || expiry >= now_ms)) {
            continue;
          }

          bool match = true;
          if (has_where) {
            const uint8_t* cptr = row_ptr + where_offset;
            if (where_type == ColType::INT || where_type == ColType::DATETIME) {
              int64_t lhs = 0;
              std::memcpy(&lhs, cptr, sizeof(int64_t));
              const int64_t rhs =
                  (where_type == ColType::INT) ? where_value.as_int : where_value.as_datetime;
              if (where_op == CompareOp::EQ) {
                match = (lhs == rhs);
              } else if (where_op == CompareOp::NE) {
                match = (lhs != rhs);
              } else if (where_op == CompareOp::LT) {
                match = (lhs < rhs);
              } else if (where_op == CompareOp::LE) {
                match = (lhs <= rhs);
              } else if (where_op == CompareOp::GT) {
                match = (lhs > rhs);
              } else {
                match = (lhs >= rhs);
              }
            } else if (where_type == ColType::DECIMAL) {
              double lhs = 0;
              std::memcpy(&lhs, cptr, sizeof(double));
              const double rhs = where_value.as_double;
              if (where_op == CompareOp::EQ) {
                match = (lhs == rhs);
              } else if (where_op == CompareOp::NE) {
                match = (lhs != rhs);
              } else if (where_op == CompareOp::LT) {
                match = (lhs < rhs);
              } else if (where_op == CompareOp::LE) {
                match = (lhs <= rhs);
              } else if (where_op == CompareOp::GT) {
                match = (lhs > rhs);
              } else {
                match = (lhs >= rhs);
              }
            } else {
              uint16_t lhs_len = 0;
              std::memcpy(&lhs_len, cptr, sizeof(uint16_t));
              const int cmp =
                  (lhs_len == where_value.as_varchar.len)
                      ? std::memcmp(cptr + 2, where_value.as_varchar.buf, lhs_len)
                      : (lhs_len < where_value.as_varchar.len ? -1 : 1);
              if (where_op == CompareOp::EQ) {
                match = (cmp == 0);
              } else if (where_op == CompareOp::NE) {
                match = (cmp != 0);
              } else if (where_op == CompareOp::LT) {
                match = (cmp < 0);
              } else if (where_op == CompareOp::LE) {
                match = (cmp <= 0);
              } else if (where_op == CompareOp::GT) {
                match = (cmp > 0);
              } else {
                match = (cmp >= 0);
              }
            }
          }

          if (!match) {
            continue;
          }
          local_count += 1;
          if (callback != nullptr) {
            if (!projectFromRowPtr(row_ptr, table, plan, local_projected)) {
              failed.store(true, std::memory_order_relaxed);
              break;
            }
            local_rows.push_back(local_projected);
          }
        }

        buffer_pool_.unpinPage(table_id, page_id);
      }

      thread_matches[t] = local_count;
      thread_results[t] = std::move(local_rows);
    });
  }

  thread_pool_.waitIdle();
  if (failed.load(std::memory_order_relaxed)) {
    return false;
  }

  for (size_t t = 0; t < workers; ++t) {
    local_matches += thread_matches[t];
    if (callback != nullptr) {
      for (const auto& row_vals : thread_results[t]) {
        if (!callback(callback_ctx, row_vals.data(), row_vals.size())) {
          keep_running = false;
          break;
        }
      }
    }
    if (!keep_running) {
      break;
    }
  }

  if (matched_rows != nullptr) {
    *matched_rows = local_matches;
  }
  return true;
}

bool ExecutionEngine::decodeRowAtOffset(const Table& table, const uint8_t* row_ptr, Row& out_row) const {
  if (row_ptr == nullptr) {
    return false;
  }
  if (row_ptr[0] == kTombstoneDeleted) {
    return false;
  }

  out_row = Row(table.schema.columns.size());
  out_row.values.resize(table.schema.columns.size());
  std::memcpy(&out_row.expiration_timestamp, row_ptr + 8, sizeof(int64_t));

  size_t cursor = kRowHeaderAlignedBytes;
  for (size_t i = 0; i < table.schema.columns.size(); ++i) {
    const ColType type = table.schema.columns[i].type;
    if (type == ColType::INT) {
      std::memcpy(&out_row.values[i].as_int, row_ptr + cursor, sizeof(int64_t));
      cursor += 8;
    } else if (type == ColType::DECIMAL) {
      std::memcpy(&out_row.values[i].as_double, row_ptr + cursor, sizeof(double));
      cursor += 8;
    } else if (type == ColType::DATETIME) {
      std::memcpy(&out_row.values[i].as_datetime, row_ptr + cursor, sizeof(int64_t));
      cursor += 8;
    } else {
      std::memcpy(&out_row.values[i].as_varchar.len, row_ptr + cursor, sizeof(uint16_t));
      std::memcpy(out_row.values[i].as_varchar.buf, row_ptr + cursor + 2, 254);
      cursor += 256;
    }
  }
  return true;
}

bool ExecutionEngine::projectFromRowPtr(const uint8_t* row_ptr,
                                        const Table& table,
                                        const QueryPlan& plan,
                                        std::vector<RowValue>& projected) const {
  if (projected.size() != plan.projected_col_indices.size() ||
      plan.projected_col_indices.size() != plan.projected_col_offsets.size()) {
    return false;
  }

  for (size_t i = 0; i < plan.projected_col_indices.size(); ++i) {
    const int col_idx = plan.projected_col_indices[i];
    const size_t col_off = plan.projected_col_offsets[i];
    const ColType type = table.schema.columns[static_cast<size_t>(col_idx)].type;
    const uint8_t* vptr = row_ptr + col_off;

    if (type == ColType::INT) {
      std::memcpy(&projected[i].as_int, vptr, sizeof(int64_t));
    } else if (type == ColType::DECIMAL) {
      std::memcpy(&projected[i].as_double, vptr, sizeof(double));
    } else if (type == ColType::DATETIME) {
      std::memcpy(&projected[i].as_datetime, vptr, sizeof(int64_t));
    } else {
      std::memcpy(&projected[i].as_varchar.len, vptr, sizeof(uint16_t));
      const uint16_t len = projected[i].as_varchar.len;
      if (len > 0) {
        std::memcpy(projected[i].as_varchar.buf, vptr + 2, len);
      }
      if (len < 254) {
        std::memset(projected[i].as_varchar.buf + len, 0, static_cast<size_t>(254 - len));
      }
    }
  }
  return true;
}

void ExecutionEngine::expirationLoop() {
  while (expiration_running_.load(std::memory_order_acquire)) {
    std::vector<std::pair<uint32_t, Table*>> tables;
    {
      std::lock_guard<std::mutex> lock(expiration_tables_mutex_);
      tables = expiration_tables_;
    }

    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();

    for (const auto& entry : tables) {
      const uint32_t table_id = entry.first;
      Table* table = entry.second;
      if (table == nullptr) {
        continue;
      }

      uint64_t tombstoned = 0;
      uint64_t total_rows = 0;
      {
        DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
        std::shared_lock<std::shared_mutex> table_lock(table->rw_lock);
        const uint32_t page_count = table->storage->pageCount();
        for (uint32_t page_id = 0; page_id < page_count; ++page_id) {
          BufferFrame* frame = buffer_pool_.fetchPage(table_id, page_id);
          if (frame == nullptr) {
            continue;
          }
          bool dirty = false;
          uint8_t* raw = reinterpret_cast<uint8_t*>(frame->page.body.data());
          for (uint16_t slot = 0; slot < frame->page.row_count; ++slot) {
            uint8_t* row_ptr = raw + static_cast<size_t>(slot) * table->row_size_bytes;
            total_rows += 1;
            if (row_ptr[0] == kTombstoneDeleted) {
              tombstoned += 1;
              continue;
            }
            int64_t expiry = 0;
            std::memcpy(&expiry, row_ptr + 8, sizeof(int64_t));
            if (expiry > 0 && expiry < now_ms) {
              row_ptr[0] = kTombstoneDeleted;
              tombstoned += 1;
              dirty = true;
            }
          }
          if (dirty) {
            buffer_pool_.markDirty(table_id, page_id);
          }
          buffer_pool_.unpinPage(table_id, page_id);
        }
      }

      if (total_rows > 0 && (static_cast<double>(tombstoned) / static_cast<double>(total_rows)) > 0.20) {
        compactTable(*table, table_id);
      }
    }

    for (int i = 0; i < 30; ++i) {
      if (!expiration_running_.load(std::memory_order_acquire)) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

bool ExecutionEngine::compactTable(Table& table, uint32_t table_id) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::unique_lock<std::shared_mutex> table_lock(table.rw_lock);

  TableRuntime& state = stateFor(table_id);
  struct FlushCtx {
    ExecutionEngine* self;
    const Table* table;
    uint32_t table_id;
  } ctx{this, &table, table_id};
  auto cb = [](void* arg, const Row& persisted_row, uint32_t page_id, uint16_t row_offset) {
    auto* c = static_cast<FlushCtx*>(arg);
    c->self->onRowPersisted(*c->table, c->table_id, persisted_row, page_id, row_offset);
  };
  if (!state.insert_buffer.flush(table, buffer_pool_, table_id, cb, &ctx)) {
    return false;
  }
  if (state.bulk_mode && !state.bulk_rows.empty()) {
    if (!appendRowsToPages(table, table_id, state.bulk_rows, nullptr, true)) {
      return false;
    }
    state.bulk_rows.clear();
    state.bulk_index.clear();
    state.bulk_mode = false;
  }

  buffer_pool_.flushAll();
  if (!table.storage->fsyncFile()) {
    return false;
  }
  buffer_pool_.evictTable(table_id);

  const std::string table_dir = "data/" + table.name;
  const std::string segment_path = table_dir + "/segment_0.db";
  const std::string legacy_segment_path = table_dir + "/segment_0.dat";
  const std::string temp_path = table_dir + "/segment_0.compact.tmp";

  StorageEngine compact_storage;
  if (!compact_storage.open(temp_path)) {
    return false;
  }
  uint32_t compact_page_id = compact_storage.allocateNewPage(table.row_size_bytes);
  if (compact_page_id == UINT32_MAX) {
    return false;
  }
  Page compact_page{};
  if (!compact_storage.readPage(compact_page_id, compact_page)) {
    return false;
  }

  std::vector<std::pair<int64_t, RecordPointer>> pairs;
  pairs.reserve(4096);
  std::unordered_map<uint64_t, BloomFilter> rebuilt_bloom;
  rebuilt_bloom.reserve(4096);
  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

  const uint32_t old_pages = table.storage->pageCount();
  for (uint32_t page_id = 0; page_id < old_pages; ++page_id) {
    Page page{};
    if (!table.storage->readPage(page_id, page)) {
      return false;
    }
    const uint8_t* raw = reinterpret_cast<const uint8_t*>(page.body.data());
    for (uint16_t slot = 0; slot < page.row_count; ++slot) {
      const uint8_t* row_ptr = raw + static_cast<size_t>(slot) * table.row_size_bytes;
      if (!rowLiveAndFresh(row_ptr, now_ms)) {
        continue;
      }
      Row row;
      if (!decodeRowAtOffset(table, row_ptr, row)) {
        continue;
      }
      if (static_cast<uint32_t>(compact_page.free_space_offset) + table.row_size_bytes > kPageBodyBytes) {
        if (!compact_storage.writePage(compact_page_id, compact_page)) {
          return false;
        }
        compact_page_id = compact_storage.allocateNewPage(table.row_size_bytes);
        if (compact_page_id == UINT32_MAX || !compact_storage.readPage(compact_page_id, compact_page)) {
          return false;
        }
      }

      const uint16_t row_offset = compact_page.free_space_offset;
      if (!serializeRowIntoPage(row, table.schema, table.row_size_bytes, compact_page)) {
        return false;
      }

      const ColType pk_type = table.schema.columns[0].type;
      const int64_t key = static_cast<int64_t>(encodePrimaryKey(pk_type, row.values[0]));
      pairs.emplace_back(key, RecordPointer{compact_page_id, row_offset});
      for (size_t col = 1; col < table.schema.columns.size(); ++col) {
        const uint64_t bkey = bloomKey(table_id, compact_page_id, static_cast<uint16_t>(col));
        rebuilt_bloom[bkey].add(valueHash(table.schema.columns[col].type, row.values[col]));
      }
    }
  }
  if (!compact_storage.writePage(compact_page_id, compact_page) || !compact_storage.fsyncFile()) {
    return false;
  }

  std::error_code ec;
  std::filesystem::remove(legacy_segment_path, ec);
  std::filesystem::remove(segment_path, ec);
  std::filesystem::rename(temp_path, segment_path, ec);
  if (ec) {
    return false;
  }

  table.storage = std::make_unique<StorageEngine>();
  if (!table.storage->open(segment_path)) {
    return false;
  }
  table.current_page_id = (table.storage->pageCount() == 0) ? 0 : (table.storage->pageCount() - 1);
  buffer_pool_.registerTable(table_id, table.storage.get());

  std::sort(pairs.begin(), pairs.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  state.bplus_tree.buildBulkFromSorted(pairs);
  state.page_bloom_filters = std::move(rebuilt_bloom);
  return true;
}

void ExecutionEngine::shutdown(std::vector<std::pair<uint32_t, Table*>> tables) {
  stopExpirationThread();
  for (const auto& t : tables) {
    flushTable(*t.second, t.first);
  }
  buffer_pool_.flushAll();
  wal_.checkpoint(wal_.lastFlushedSeq());
  wal_.stop();
  for (auto& kv : runtime_) {
    kv.second.pending_insert_bytes = 0;
  }
}

bool ExecutionEngine::checkpoint(Table& table, uint32_t table_id) {
  if (!flushTable(table, table_id)) {
    return false;
  }
  const uint64_t seq = wal_.lastFlushedSeq();
  const bool ok = wal_.checkpoint(seq);
  if (ok) {
    stateFor(table_id).pending_insert_bytes = 0;
  }
  return ok;
}

bool ExecutionEngine::insertRecoveredRow(Table& table, uint32_t table_id, const Row& row) {
  std::vector<Row> one;
  one.push_back(row);
  return appendRowsToPages(table, table_id, one, nullptr, true);
}

bool ExecutionEngine::recoverTable(Table& table, uint32_t table_id) {
  DebugLockLevelGuard table_level(LockLevel::TABLE_RW);
  std::unique_lock<std::shared_mutex> table_lock(table.rw_lock);
  std::vector<WALManager::RecoveredInsert> recovered;
  if (!wal_.recoverTable(table.name, table.schema, recovered)) {
    return false;
  }

  for (const auto& rec : recovered) {
    if (!insertRecoveredRow(table, table_id, rec.row)) {
      return false;
    }
  }
  buffer_pool_.flushAll();
  return table.storage->fsyncFile();
}

}  // namespace flexql
