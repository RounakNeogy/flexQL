#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <thread>
#include <deque>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "flexql/buffer_pool.hpp"
#include "flexql/bloom_filter.hpp"
#include "flexql/bplus_tree.hpp"
#include "flexql/core_types.hpp"
#include "flexql/insert_buffer.hpp"
#include "flexql/query_ast.hpp"
#include "flexql/query_planner.hpp"
#include "flexql/thread_pool.hpp"
#include "flexql/wal_manager.hpp"

namespace flexql {

class ExecutionEngine {
 public:
  using SelectRowCallback = bool (*)(void* ctx, const RowValue* projected_values, size_t projected_count);

  explicit ExecutionEngine(BufferPool& buffer_pool);
  ~ExecutionEngine();

  bool executeInsert(Table& table, uint32_t table_id, const Row& row);
  bool executeInsertBatch(Table& table, uint32_t table_id, std::vector<Row>& rows);
  bool executeBulkInsert(Table& table, uint32_t table_id, std::vector<Row>& rows);
  bool executeSelectByPrimaryKey(Table& table, uint32_t table_id, const RowValue& pk_val, Row& out_row);
  bool executeSelectAll(const QueryAST& ast,
                        Table& table,
                        uint32_t table_id,
                        SelectRowCallback callback,
                        void* callback_ctx,
                        size_t* matched_rows);
  bool executeSelectWhere(const QueryAST& ast,
                          Table& table,
                          uint32_t table_id,
                          SelectRowCallback callback,
                          void* callback_ctx,
                          size_t* matched_rows);
  bool execute(const QueryAST& ast,
               Table& table,
               uint32_t table_id,
               SelectRowCallback callback,
               void* callback_ctx,
               size_t* matched_rows);
  bool executeJoin(const QueryAST& ast,
                   Table& left_table,
                   uint32_t left_table_id,
                   Table& right_table,
                   uint32_t right_table_id,
                   SelectRowCallback callback,
                   void* callback_ctx,
                   size_t* matched_rows);

  bool beginBulkLoad(uint32_t table_id);
  bool endBulkLoad(Table& table, uint32_t table_id);
  bool flushTable(Table& table, uint32_t table_id);
  void resetTableRuntime(uint32_t table_id);
  bool warmupTableRuntimeFromDisk(Table& table, uint32_t table_id);
  void startExpirationThread(const std::vector<std::pair<uint32_t, Table*>>& tables);
  void stopExpirationThread();
  bool checkpoint(Table& table, uint32_t table_id);
  bool recoverTable(Table& table, uint32_t table_id);
  void shutdown(std::vector<std::pair<uint32_t, Table*>> tables);
  bool primaryKeyExists(Table& table, uint32_t table_id, const RowValue& pk_value);
  bool hasPendingWrites(uint32_t table_id);
  bool primaryKeyRuntimeEmpty(uint32_t table_id);

 private:
  struct TableRuntime {
    InsertBuffer insert_buffer;
    bool bulk_mode;
    std::vector<Row> bulk_rows;
    std::unordered_map<int64_t, Row*> bulk_index;
    std::unordered_map<int64_t, uint64_t> primary_index;
    BPlusTree bplus_tree;
    std::unordered_map<uint64_t, BloomFilter> page_bloom_filters;
    std::deque<uint64_t> recent_insert_micros;
    uint64_t tombstoned_rows;
    uint64_t live_rows;
    uint64_t pending_insert_bytes;

    TableRuntime();
  };

  BufferPool& buffer_pool_;
  std::unordered_map<uint32_t, TableRuntime> runtime_;
  WALManager wal_;
  std::atomic<uint64_t> insert_counter_;
  ThreadPool thread_pool_;
  std::atomic<bool> expiration_running_;
  std::thread expiration_thread_;
  std::mutex expiration_tables_mutex_;
  std::vector<std::pair<uint32_t, Table*>> expiration_tables_;

  static constexpr size_t kAdaptiveBulkThreshold = 1000;
  static constexpr uint64_t kAdaptiveWindowMicros = 200000;
  static constexpr uint64_t kCheckpointBytes = 128ULL * 1024ULL * 1024ULL;

  TableRuntime& stateFor(uint32_t table_id);
  static uint64_t nowMicros();

  bool appendRowsToPages(Table& table,
                         uint32_t table_id,
                         const std::vector<Row>& rows,
                         std::vector<std::pair<int64_t, RecordPointer>>* captured_pairs,
                         bool incremental_index_update);
  void maybeEnableAdaptiveBulk(uint32_t table_id);
  bool writeBulkLoadWalMarker(const Table& table, size_t row_count);
  void buildPrimaryIndexOnePass(uint32_t table_id, ColType pk_type, const std::vector<Row>& rows);
  bool selectFromDisk(Table& table, uint32_t table_id, const RowValue& pk_val, Row& out_row);
  bool executeSelectScan(const QueryAST& ast,
                         Table& table,
                         uint32_t table_id,
                         SelectRowCallback callback,
                         void* callback_ctx,
                         size_t* matched_rows);
  bool executePlannedScan(const QueryAST& ast,
                          const QueryPlan& plan,
                          Table& table,
                          uint32_t table_id,
                          SelectRowCallback callback,
                          void* callback_ctx,
                          size_t* matched_rows);
  void onRowPersisted(const Table& table,
                      uint32_t table_id,
                      const Row& row,
                      uint32_t page_id,
                      uint16_t row_offset);
  static uint64_t bloomKey(uint32_t table_id, uint32_t page_id, uint16_t col_index);
  static uint64_t valueHash(ColType type, const RowValue& value);
  bool decodeRowAtOffset(const Table& table, const uint8_t* row_ptr, Row& out_row) const;
  bool projectFromRowPtr(const uint8_t* row_ptr,
                         const Table& table,
                         const QueryPlan& plan,
                         std::vector<RowValue>& projected) const;
  bool insertRecoveredRow(Table& table, uint32_t table_id, const Row& row);
  static bool rowPassesCondition(const uint8_t* row_ptr,
                                 const Table& table,
                                 const Condition& cond,
                                 const std::vector<size_t>& col_offsets);
  static bool rowLiveAndFresh(const uint8_t* row_ptr, int64_t now_ms);
  static void readColumnValue(const uint8_t* row_ptr, size_t offset, ColType type, RowValue& out_value);
  static bool compareValues(CompareOp op, ColType type, const RowValue& lhs, const RowValue& rhs);
  static bool equalValuesForJoin(ColType type, const RowValue& lhs, const RowValue& rhs);
  bool emitJoinProjectedRow(const QueryAST& ast,
                            const uint8_t* left_row_ptr,
                            const Table& left_table,
                            const uint8_t* right_row_ptr,
                            const Table& right_table,
                            std::vector<RowValue>& out_projected) const;
  void expirationLoop();
  bool compactTable(Table& table, uint32_t table_id);
};

}  // namespace flexql
