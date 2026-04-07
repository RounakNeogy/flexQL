#include "flexql/wal_manager.hpp"

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "flexql/lock_order.hpp"

namespace flexql {

namespace {

template <typename T>
void appendPod(std::string& out, T value) {
  const char* p = reinterpret_cast<const char*>(&value);
  out.append(p, p + sizeof(T));
}

template <typename T>
bool readPod(const uint8_t*& p, const uint8_t* end, T& out) {
  if (static_cast<size_t>(end - p) < sizeof(T)) {
    return false;
  }
  std::memcpy(&out, p, sizeof(T));
  p += sizeof(T);
  return true;
}

}  // namespace

WALManager::WALManager(std::string wal_path)
    : wal_path_(std::move(wal_path)),
      fd_(-1),
      active_buffer_(),
      flush_buffer_(),
      active_seqs_(),
      flush_seqs_(),
      active_bytes_(0),
      flush_bytes_(0),
      mtx_(),
      cv_(),
      cv_flushed_(),
      stop_flag_(false),
      started_(false),
      force_flush_(false),
      flush_thread_(),
      seq_counter_(1),
      last_flushed_seq_(0) {
  active_buffer_.reserve(kFlushEntryThreshold);
  flush_buffer_.reserve(kFlushEntryThreshold);
  active_seqs_.reserve(kFlushEntryThreshold);
  flush_seqs_.reserve(kFlushEntryThreshold);
}

WALManager::~WALManager() {
  stop();
}

bool WALManager::openIfNeeded() {
  if (fd_ >= 0) {
    return true;
  }

  std::filesystem::path p(wal_path_);
  std::error_code ec;
  std::filesystem::create_directories(p.parent_path(), ec);
  if (ec) {
    return false;
  }

  fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  return fd_ >= 0;
}

bool WALManager::start() {
  DebugLockLevelGuard level_guard(LockLevel::WAL);
  std::lock_guard<std::mutex> lock(mtx_);
  if (started_) {
    return true;
  }
  if (!openIfNeeded()) {
    return false;
  }

  stop_flag_ = false;
  force_flush_ = false;
  started_ = true;
  flush_thread_ = std::thread(&WALManager::flushThreadMain, this);
  return true;
}

void WALManager::stop() {
  {
    DebugLockLevelGuard level_guard(LockLevel::WAL);
    std::lock_guard<std::mutex> lock(mtx_);
    if (!started_) {
      if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
      }
      return;
    }
    stop_flag_ = true;
    force_flush_ = true;
  }
  cv_.notify_all();

  if (flush_thread_.joinable()) {
    flush_thread_.join();
  }

  started_ = false;
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

uint64_t WALManager::nextSeq() {
  return seq_counter_.fetch_add(1, std::memory_order_relaxed);
}

std::string WALManager::formatInsertBinary(uint64_t seq,
                                           const std::string& table_name,
                                           const Schema& schema,
                                           const Row& row) const {
  std::string payload;
  payload.reserve(64 + table_name.size() + schema.columns.size() * 16);

  appendPod(payload, seq);
  const uint16_t tlen = static_cast<uint16_t>(table_name.size());
  appendPod(payload, tlen);
  payload.append(table_name.data(), table_name.data() + tlen);
  appendPod(payload, row.expiration_timestamp);
  const uint16_t col_count = static_cast<uint16_t>(schema.columns.size());
  appendPod(payload, col_count);

  for (size_t i = 0; i < schema.columns.size(); ++i) {
    const ColType t = schema.columns[i].type;
    payload.push_back(static_cast<char>(t));
    if (t == ColType::INT) {
      appendPod(payload, row.values[i].as_int);
    } else if (t == ColType::DECIMAL) {
      appendPod(payload, row.values[i].as_double);
    } else if (t == ColType::DATETIME) {
      appendPod(payload, row.values[i].as_datetime);
    } else {
      const uint16_t len = row.values[i].as_varchar.len;
      appendPod(payload, len);
      payload.append(row.values[i].as_varchar.buf, row.values[i].as_varchar.buf + len);
    }
  }

  std::string rec;
  rec.reserve(1 + 4 + payload.size());
  rec.push_back(static_cast<char>(kRecordInsert));
  const uint32_t plen = static_cast<uint32_t>(payload.size());
  appendPod(rec, plen);
  rec.append(payload);
  return rec;
}

std::string WALManager::formatCheckpointBinary(uint64_t seq) const {
  std::string rec;
  rec.reserve(1 + 4 + 8);
  rec.push_back(static_cast<char>(kRecordCheckpoint));
  const uint32_t plen = sizeof(uint64_t);
  appendPod(rec, plen);
  appendPod(rec, seq);
  return rec;
}

bool WALManager::parseCheckpointBinary(const uint8_t* payload, size_t payload_len, uint64_t& out_seq) const {
  if (payload_len != sizeof(uint64_t)) {
    return false;
  }
  std::memcpy(&out_seq, payload, sizeof(uint64_t));
  return true;
}

bool WALManager::parseInsertBinary(const uint8_t* payload,
                                   size_t payload_len,
                                   const std::string& table_name,
                                   const Schema& schema,
                                   RecoveredInsert& out) const {
  const uint8_t* p = payload;
  const uint8_t* end = payload + payload_len;

  uint64_t seq = 0;
  uint16_t tlen = 0;
  int64_t expiry = 0;
  uint16_t col_count = 0;
  if (!readPod(p, end, seq) || !readPod(p, end, tlen)) {
    return false;
  }
  if (static_cast<size_t>(end - p) < tlen) {
    return false;
  }
  const std::string rec_table(reinterpret_cast<const char*>(p), reinterpret_cast<const char*>(p + tlen));
  p += tlen;
  if (!readPod(p, end, expiry) || !readPod(p, end, col_count)) {
    return false;
  }
  if (rec_table != table_name || col_count != schema.columns.size()) {
    return false;
  }

  out.seq = seq;
  out.table_name = rec_table;
  out.row = Row(schema.columns.size());
  out.row.values.resize(schema.columns.size());
  out.row.expiration_timestamp = expiry;

  for (size_t i = 0; i < schema.columns.size(); ++i) {
    uint8_t raw_t = 0;
    if (!readPod(p, end, raw_t)) {
      return false;
    }
    const ColType t = static_cast<ColType>(raw_t);
    if (t != schema.columns[i].type) {
      return false;
    }
    if (t == ColType::INT) {
      if (!readPod(p, end, out.row.values[i].as_int)) return false;
    } else if (t == ColType::DECIMAL) {
      if (!readPod(p, end, out.row.values[i].as_double)) return false;
    } else if (t == ColType::DATETIME) {
      if (!readPod(p, end, out.row.values[i].as_datetime)) return false;
    } else {
      uint16_t len = 0;
      if (!readPod(p, end, len) || static_cast<size_t>(end - p) < len) {
        return false;
      }
      out.row.values[i] = RowValue::from_varchar(reinterpret_cast<const char*>(p), len);
      p += len;
    }
  }
  return true;
}

uint64_t WALManager::appendInsert(const std::string& table_name, const Schema& schema, const Row& row) {
  const uint64_t seq = nextSeq();
  const std::string rec = formatInsertBinary(seq, table_name, schema, row);

  {
    DebugLockLevelGuard level_guard(LockLevel::WAL);
    std::lock_guard<std::mutex> lock(mtx_);
    active_buffer_.push_back(rec);
    active_seqs_.push_back(seq);
    active_bytes_ += rec.size();
    if (active_buffer_.size() >= kFlushEntryThreshold || active_bytes_ >= kFlushByteThreshold) {
      force_flush_ = true;
    }
  }
  cv_.notify_one();
  return seq;
}

uint64_t WALManager::appendInsertBatch(const std::string& table_name,
                                        const Schema& schema,
                                        const std::vector<Row>& rows) {
  if (rows.empty()) {
    return lastFlushedSeq();
  }

  // Concatenate all row records into a single string to avoid per-row allocations.
  std::string combined;
  combined.reserve(rows.size() * 64);
  uint64_t first_seq = 0;
  uint64_t last_seq = 0;
  {
    DebugLockLevelGuard level_guard(LockLevel::WAL);
    std::lock_guard<std::mutex> lock(mtx_);
    first_seq = nextSeq();
    // Reserve seq range
    for (size_t i = 1; i < rows.size(); ++i) {
      nextSeq();
    }
    last_seq = first_seq + rows.size() - 1;

    for (size_t i = 0; i < rows.size(); ++i) {
      combined.append(formatInsertBinary(first_seq + i, table_name, schema, rows[i]));
    }
    active_buffer_.push_back(std::move(combined));
    active_seqs_.push_back(last_seq);
    active_bytes_ += active_buffer_.back().size();
    if (active_buffer_.size() >= kFlushEntryThreshold || active_bytes_ >= kFlushByteThreshold) {
      force_flush_ = true;
    }
  }
  cv_.notify_one();
  return last_seq;
}

bool WALManager::waitUntilFlushed(uint64_t seq) {
  DebugLockLevelGuard level_guard(LockLevel::WAL);
  std::unique_lock<std::mutex> lock(mtx_);
  cv_flushed_.wait(lock, [&]() {
    return last_flushed_seq_.load(std::memory_order_relaxed) >= seq || !started_;
  });
  return last_flushed_seq_.load(std::memory_order_relaxed) >= seq;
}

uint64_t WALManager::lastFlushedSeq() const {
  return last_flushed_seq_.load(std::memory_order_relaxed);
}

bool WALManager::flushBatch(std::vector<std::string>& lines, std::vector<uint64_t>& seqs) {
  if (lines.empty()) {
    return true;
  }
  if (!openIfNeeded()) {
    return false;
  }

  for (const std::string& line : lines) {
    const char* p = line.data();
    size_t left = line.size();
    while (left > 0) {
      const ssize_t rc = ::write(fd_, p, left);
      if (rc < 0) {
        if (errno == EINTR) {
          continue;
        }
        return false;
      }
      p += static_cast<size_t>(rc);
      left -= static_cast<size_t>(rc);
    }
  }

#ifdef __APPLE__
  if (::fsync(fd_) != 0) {
#else
  if (::fdatasync(fd_) != 0) {
#endif
    return false;
  }

  uint64_t max_seq = 0;
  for (uint64_t seq : seqs) {
    if (seq > max_seq) {
      max_seq = seq;
    }
  }
  if (max_seq > 0) {
    last_flushed_seq_.store(max_seq, std::memory_order_relaxed);
  }
  return true;
}

void WALManager::flushThreadMain() {
  while (true) {
    {
      DebugLockLevelGuard level_guard(LockLevel::WAL);
      std::unique_lock<std::mutex> lock(mtx_);
      cv_.wait_for(lock, std::chrono::milliseconds(20), [&]() {
        return stop_flag_ || force_flush_ || active_buffer_.size() >= kFlushEntryThreshold ||
               active_bytes_ >= kFlushByteThreshold;
      });

      if (active_buffer_.empty() && !stop_flag_) {
        continue;
      }

      flush_buffer_.swap(active_buffer_);
      flush_seqs_.swap(active_seqs_);
      flush_bytes_ = active_bytes_;
      active_bytes_ = 0;
      force_flush_ = false;
    }

    flushBatch(flush_buffer_, flush_seqs_);
    flush_buffer_.clear();
    flush_seqs_.clear();
    flush_bytes_ = 0;

    cv_flushed_.notify_all();

    std::lock_guard<std::mutex> lock(mtx_);
    if (stop_flag_ && active_buffer_.empty()) {
      break;
    }
  }
}

bool WALManager::forceFlush() {
  {
    DebugLockLevelGuard level_guard(LockLevel::WAL);
    std::lock_guard<std::mutex> lock(mtx_);
    force_flush_ = true;
  }
  cv_.notify_one();

  DebugLockLevelGuard level_guard(LockLevel::WAL);
  std::unique_lock<std::mutex> lock(mtx_);
  cv_flushed_.wait_for(lock, std::chrono::milliseconds(1000), [&]() { return active_buffer_.empty(); });
  return true;
}

bool WALManager::checkpoint(uint64_t seq) {
  {
    DebugLockLevelGuard level_guard(LockLevel::WAL);
    std::lock_guard<std::mutex> lock(mtx_);
    active_buffer_.push_back(formatCheckpointBinary(seq));
    active_seqs_.push_back(seq);
    active_bytes_ += active_buffer_.back().size();
    force_flush_ = true;
  }
  cv_.notify_one();
  waitUntilFlushed(seq);

  const std::string marker = formatCheckpointBinary(seq);
  const int tmp_fd = ::open(wal_path_.c_str(), O_WRONLY | O_TRUNC, 0644);
  if (tmp_fd < 0) {
    return false;
  }

  const char* p = marker.data();
  size_t left = marker.size();
  while (left > 0) {
    const ssize_t rc = ::write(tmp_fd, p, left);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      ::close(tmp_fd);
      return false;
    }
    p += static_cast<size_t>(rc);
    left -= static_cast<size_t>(rc);
  }

#ifdef __APPLE__
  const bool ok = (::fsync(tmp_fd) == 0);
#else
  const bool ok = (::fdatasync(tmp_fd) == 0);
#endif
  ::close(tmp_fd);
  return ok;
}

bool WALManager::recoverTable(const std::string& table_name,
                              const Schema& schema,
                              std::vector<RecoveredInsert>& out) {
  out.clear();

  std::ifstream in(wal_path_, std::ios::binary);
  if (!in.good()) {
    return true;
  }

  std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  const uint8_t* p = bytes.data();
  const uint8_t* end = bytes.data() + bytes.size();

  uint64_t checkpoint_seq = 0;
  while (static_cast<size_t>(end - p) >= 5) {
    uint8_t rec_type = *p++;
    uint32_t plen = 0;
    std::memcpy(&plen, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    if (static_cast<size_t>(end - p) < plen) {
      break;
    }
    if (rec_type == kRecordCheckpoint) {
      uint64_t seq = 0;
      if (parseCheckpointBinary(p, plen, seq) && seq > checkpoint_seq) {
        checkpoint_seq = seq;
      }
    }
    p += plen;
  }

  p = bytes.data();
  while (static_cast<size_t>(end - p) >= 5) {
    uint8_t rec_type = *p++;
    uint32_t plen = 0;
    std::memcpy(&plen, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    if (static_cast<size_t>(end - p) < plen) {
      break;
    }
    if (rec_type == kRecordInsert) {
      RecoveredInsert rec;
      if (parseInsertBinary(p, plen, table_name, schema, rec) && rec.seq > checkpoint_seq) {
        out.push_back(std::move(rec));
      }
    }
    p += plen;
  }
  return true;
}

}  // namespace flexql
