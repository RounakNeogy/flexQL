#pragma once

#include <atomic>
#include <cstddef>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "flexql/core_types.hpp"

namespace flexql {

class WALManager {
 public:
  struct RecoveredInsert {
    uint64_t seq;
    std::string table_name;
    Row row;
  };

  explicit WALManager(std::string wal_path = "data/wal.log");
  ~WALManager();

  bool start();
  void stop();

  uint64_t appendInsert(const std::string& table_name, const Schema& schema, const Row& row);
  uint64_t appendInsertBatch(const std::string& table_name, const Schema& schema, const std::vector<Row>& rows);
  bool waitUntilFlushed(uint64_t seq);
  uint64_t lastFlushedSeq() const;

  bool checkpoint(uint64_t seq);
  bool recoverTable(const std::string& table_name, const Schema& schema, std::vector<RecoveredInsert>& out);

 private:
  uint64_t nextSeq();
  std::string formatInsertBinary(uint64_t seq,
                                 const std::string& table_name,
                                 const Schema& schema,
                                 const Row& row) const;
  std::string formatCheckpointBinary(uint64_t seq) const;
  bool parseInsertBinary(const uint8_t* payload,
                         size_t payload_len,
                         const std::string& table_name,
                         const Schema& schema,
                         RecoveredInsert& out) const;
  bool parseCheckpointBinary(const uint8_t* payload, size_t payload_len, uint64_t& out_seq) const;

  bool openIfNeeded();
  void flushThreadMain();
  bool flushBatch(std::vector<std::string>& lines, std::vector<uint64_t>& seqs);
  bool forceFlush();

  std::string wal_path_;
  int fd_;

  std::vector<std::string> active_buffer_;
  std::vector<std::string> flush_buffer_;
  std::vector<uint64_t> active_seqs_;
  std::vector<uint64_t> flush_seqs_;
  size_t active_bytes_;
  size_t flush_bytes_;

  std::mutex mtx_;
  std::condition_variable cv_;
  std::condition_variable cv_flushed_;
  bool stop_flag_;
  bool started_;
  bool force_flush_;

  std::thread flush_thread_;
  std::atomic<uint64_t> seq_counter_;
  std::atomic<uint64_t> last_flushed_seq_;

  static constexpr uint8_t kRecordInsert = 1;
  static constexpr uint8_t kRecordCheckpoint = 2;
  static constexpr uint32_t kFlushByteThreshold = 16U * 1024U * 1024U;
  static constexpr uint32_t kFlushEntryThreshold = 16384U;
};

}  // namespace flexql
