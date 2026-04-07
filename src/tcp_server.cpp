#include "flexql/tcp_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "flexql/row_codec.hpp"
#include "flexql/tcp_protocol.hpp"

namespace flexql {

namespace {

constexpr size_t kRowBatchFlushBytes =
    4U << 20; // 4MB batch for fewer socket writes

bool writeAll(int fd, const void *data, size_t len) {
  const uint8_t *ptr = static_cast<const uint8_t *>(data);
  size_t written = 0;
  while (written < len) {
    const ssize_t rc = ::send(fd, ptr + written, len - written, 0);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (rc == 0) {
      return false;
    }
    written += static_cast<size_t>(rc);
  }
  return true;
}

bool readAll(int fd, void *data, size_t len) {
  uint8_t *ptr = static_cast<uint8_t *>(data);
  size_t got = 0;
  while (got < len) {
    const ssize_t rc = ::recv(fd, ptr + got, len - got, 0);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (rc == 0) {
      return false;
    }
    got += static_cast<size_t>(rc);
  }
  return true;
}

bool sendFrame(int fd, MessageType type, const uint8_t *payload,
               uint32_t payload_len) {
  const uint32_t net_len = htonl(payload_len);
  std::vector<uint8_t> frame(sizeof(uint32_t) + 1 + payload_len, 0);
  std::memcpy(frame.data(), &net_len, sizeof(uint32_t));
  frame[4] = static_cast<uint8_t>(type);
  if (payload_len > 0) {
    std::memcpy(frame.data() + 5, payload, payload_len);
  }
  return writeAll(fd, frame.data(), frame.size());
}

bool sendErrorFrame(int fd, const std::string &err) {
  return sendFrame(fd, MessageType::ERROR,
                   reinterpret_cast<const uint8_t *>(err.data()),
                   static_cast<uint32_t>(err.size()));
}

bool consumeAbortFrameIfAvailable(int fd) {
  pollfd pfd{};
  pfd.fd = fd;
  pfd.events = POLLIN;
  const int prc = ::poll(&pfd, 1, 0);
  if (prc <= 0 || (pfd.revents & POLLIN) == 0) {
    return false;
  }

  uint8_t header[5];
  const ssize_t got =
      ::recv(fd, header, sizeof(header), MSG_PEEK | MSG_DONTWAIT);
  if (got < static_cast<ssize_t>(sizeof(header))) {
    return false;
  }
  uint32_t payload_len = 0;
  std::memcpy(&payload_len, header, sizeof(uint32_t));
  payload_len = ntohl(payload_len);
  const MessageType type = static_cast<MessageType>(header[4]);
  if (type != MessageType::ABORT) {
    return false;
  }

  const size_t total = sizeof(header) + static_cast<size_t>(payload_len);
  std::vector<uint8_t> sink(total, 0);
  const ssize_t consumed = ::recv(fd, sink.data(), total, MSG_DONTWAIT);
  return consumed == static_cast<ssize_t>(total);
}

bool sendSchemaFrame(int fd, const std::vector<std::string> &names) {
  size_t payload_len = sizeof(uint16_t);
  for (const auto &name : names) {
    payload_len += sizeof(uint16_t) + name.size();
  }
  if (payload_len > UINT32_MAX) {
    return false;
  }

  std::vector<uint8_t> payload(payload_len, 0);
  uint16_t col_count = static_cast<uint16_t>(names.size());
  std::memcpy(payload.data(), &col_count, sizeof(uint16_t));
  size_t cursor = sizeof(uint16_t);
  for (const auto &name : names) {
    const uint16_t len = static_cast<uint16_t>(name.size());
    std::memcpy(payload.data() + cursor, &len, sizeof(uint16_t));
    cursor += sizeof(uint16_t);
    if (len > 0) {
      std::memcpy(payload.data() + cursor, name.data(), len);
      cursor += len;
    }
  }
  return sendFrame(fd, MessageType::SCHEMA, payload.data(),
                   static_cast<uint32_t>(payload.size()));
}

size_t encodedValueSize(ColType type) {
  return (type == ColType::VARCHAR) ? 256U : 8U;
}

struct CollectRowsContext {
  std::vector<std::vector<RowValue>> rows;
  size_t projected_count;
};

bool collectRowsCallback(void *ctx, const RowValue *projected_values,
                         size_t projected_count) {
  auto *c = static_cast<CollectRowsContext *>(ctx);
  if (c == nullptr || projected_values == nullptr ||
      projected_count != c->projected_count) {
    return false;
  }
  std::vector<RowValue> row(projected_count);
  for (size_t i = 0; i < projected_count; ++i) {
    row[i] = projected_values[i];
  }
  c->rows.emplace_back(std::move(row));
  return true;
}

int compareRowValue(ColType type, const RowValue &a, const RowValue &b) {
  if (type == ColType::INT || type == ColType::DATETIME) {
    const int64_t av = (type == ColType::INT) ? a.as_int : a.as_datetime;
    const int64_t bv = (type == ColType::INT) ? b.as_int : b.as_datetime;
    if (av < bv)
      return -1;
    if (av > bv)
      return 1;
    return 0;
  }
  if (type == ColType::DECIMAL) {
    if (a.as_double < b.as_double)
      return -1;
    if (a.as_double > b.as_double)
      return 1;
    return 0;
  }
  const uint16_t al = a.as_varchar.len;
  const uint16_t bl = b.as_varchar.len;
  const uint16_t ml = (al < bl) ? al : bl;
  const int cmp =
      (ml > 0) ? std::memcmp(a.as_varchar.buf, b.as_varchar.buf, ml) : 0;
  if (cmp != 0)
    return cmp;
  if (al < bl)
    return -1;
  if (al > bl)
    return 1;
  return 0;
}

std::string colTypeToString(ColType t) {
  if (t == ColType::INT)
    return "INT";
  if (t == ColType::DECIMAL)
    return "DECIMAL";
  if (t == ColType::VARCHAR)
    return "VARCHAR";
  return "DATETIME";
}

bool stringToColType(const std::string &s, ColType &out) {
  if (s == "INT") {
    out = ColType::INT;
    return true;
  }
  if (s == "DECIMAL") {
    out = ColType::DECIMAL;
    return true;
  }
  if (s == "VARCHAR") {
    out = ColType::VARCHAR;
    return true;
  }
  if (s == "DATETIME") {
    out = ColType::DATETIME;
    return true;
  }
  return false;
}

} // namespace

FlexQLServer::FlexQLServer(int port, size_t worker_threads)
    : port_(port), listen_fd_(-1), running_(false), accept_thread_(),
      workers_(worker_threads == 0 ? ThreadPool() : ThreadPool(worker_threads)),
      db_(), buffer_pool_(131072), engine_(buffer_pool_), parser_(),
      catalog_mutex_(), table_ids_(), next_table_id_(1) {}

FlexQLServer::~FlexQLServer() { stop(); }

bool FlexQLServer::isRunning() const {
  return running_.load(std::memory_order_acquire);
}

bool FlexQLServer::start() {
  if (isRunning()) {
    return true;
  }

  if (!loadCatalog()) {
    return false;
  }

  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    return false;
  }

  int on = 1;
  if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) !=
      0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }
  (void)::setsockopt(listen_fd_, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port_));
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) !=
      0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  if (::listen(listen_fd_, 256) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  running_.store(true, std::memory_order_release);
  accept_thread_ = std::thread([this]() { acceptLoop(); });
  return true;
}

void FlexQLServer::stop() {
  const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
  if (!was_running) {
    return;
  }
  if (listen_fd_ >= 0) {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
  workers_.waitIdle();
  workers_.shutdown();

  std::vector<std::pair<uint32_t, Table *>> engines_tables;
  {
    std::lock_guard<std::mutex> lock(catalog_mutex_);
    for (const auto &kv : table_ids_) {
      Table *t = const_cast<Database &>(db_).getTable(kv.first);
      if (t != nullptr) {
        engines_tables.emplace_back(kv.second, t);
      }
    }
  }
  engine_.shutdown(engines_tables);
}

void FlexQLServer::acceptLoop() {
  while (isRunning()) {
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    const int fd =
        ::accept(listen_fd_, reinterpret_cast<sockaddr *>(&peer), &peer_len);
    if (fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (!isRunning()) {
        break;
      }
      continue;
    }

    int on = 1;
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    workers_.submit([this, fd]() { handleConnection(fd); });
  }
}

std::unordered_map<std::string, const Table *>
FlexQLServer::snapshotTables() const {
  std::unordered_map<std::string, const Table *> out;
  std::lock_guard<std::mutex> lock(catalog_mutex_);
  out.reserve(table_ids_.size());
  for (const auto &kv : table_ids_) {
    Table *t = const_cast<Database &>(db_).getTable(kv.first);
    if (t != nullptr) {
      out.emplace(kv.first, t);
    }
  }
  return out;
}

bool FlexQLServer::resolveTable(const std::string &name, Table *&out_table,
                                uint32_t &out_table_id) const {
  std::lock_guard<std::mutex> lock(catalog_mutex_);
  const auto it = table_ids_.find(name);
  if (it == table_ids_.end()) {
    return false;
  }
  out_table = const_cast<Database &>(db_).getTable(name);
  out_table_id = it->second;
  return out_table != nullptr;
}

bool FlexQLServer::handleCreate(const QueryAST &ast, std::string &error) {
  if (ast.create_if_not_exists) {
    Table *existing = db_.getTable(ast.table_name);
    if (existing != nullptr) {
      return true;
    }
  }
  Schema schema(ast.create_columns.size());
  for (const auto &col : ast.create_columns) {
    if (!schema.add_column(col.name, col.type)) {
      error = "duplicate column name: " + col.name;
      return false;
    }
  }
  if (!db_.createTable(ast.table_name, schema)) {
    error = "failed to create table: " + ast.table_name;
    return false;
  }
  Table *table = db_.getTable(ast.table_name);
  if (table == nullptr) {
    error = "failed to resolve table after create";
    return false;
  }
  const uint32_t table_id =
      next_table_id_.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(catalog_mutex_);
    table_ids_[ast.table_name] = table_id;
  }
  buffer_pool_.registerTable(table_id, table->storage.get());
  if (!persistCatalog()) {
    error = "failed to persist catalog";
    return false;
  }
  return true;
}

bool FlexQLServer::handleInsert(const QueryAST &ast, std::string &error) {
  Table *table = nullptr;
  uint32_t table_id = 0;
  if (!resolveTable(ast.table_name, table, table_id)) {
    error = "unknown table: " + ast.table_name;
    return false;
  }

  std::unordered_set<uint64_t> statement_pk_seen;
  statement_pk_seen.reserve(ast.insert_rows.empty() ? 1
                                                    : ast.insert_rows.size());
  const bool runtime_pk_empty = engine_.primaryKeyRuntimeEmpty(table_id);

  if (!ast.insert_rows.empty()) {
    static constexpr size_t kBulkStatementThreshold = 4096;
    std::vector<Row> batch_rows;
    batch_rows.reserve(ast.insert_rows.size());
    for (const auto &vals : ast.insert_rows) {
      const uint64_t pk_key =
          encodePrimaryKey(table->schema.columns[0].type, vals[0]);
      const bool duplicate_in_statement =
          !statement_pk_seen.insert(pk_key).second;
      // Skip the expensive B+ tree lookup when the runtime index is empty;
      // the in-statement uniqueness check is sufficient in that case.
      const bool duplicate_existing =
          !runtime_pk_empty &&
          engine_.primaryKeyExists(*table, table_id, vals[0]);
      if (duplicate_in_statement || duplicate_existing) {
        error = "duplicate primary key";
        return false;
      }
      Row row(table->schema.columns.size());
      row.values = vals;
      row.expiration_timestamp = ast.insert_expiration_timestamp;
      batch_rows.emplace_back(std::move(row));
    }
    const bool ok =
        (batch_rows.size() >= kBulkStatementThreshold)
            ? engine_.executeBulkInsert(*table, table_id, batch_rows)
            : engine_.executeInsertBatch(*table, table_id, batch_rows);
    if (!ok) {
      error = "insert failed";
      return false;
    }
  } else {
    if (!runtime_pk_empty &&
        engine_.primaryKeyExists(*table, table_id, ast.insert_values[0])) {
      error = "duplicate primary key";
      return false;
    }
    Row row(table->schema.columns.size());
    row.values = ast.insert_values;
    row.expiration_timestamp = ast.insert_expiration_timestamp;
    if (!engine_.executeInsert(*table, table_id, row)) {
      error = "insert failed";
      return false;
    }
  }
  return true;
}

bool FlexQLServer::handleDelete(const QueryAST &ast, std::string &error) {
  Table *table = nullptr;
  uint32_t table_id = 0;
  if (!resolveTable(ast.table_name, table, table_id)) {
    error = "unknown table: " + ast.table_name;
    return false;
  }

  if (!engine_.flushTable(*table, table_id)) {
    error = "failed to flush table before delete";
    return false;
  }
  buffer_pool_.evictTable(table_id);

  const std::string segment_db = "data/" + table->name + "/segment_0.db";
  const std::string segment_dat = "data/" + table->name + "/segment_0.dat";
  const std::string schema_txt = "data/" + table->name + "/schema.txt";
  std::error_code ec;
  std::filesystem::remove(segment_db, ec);
  std::filesystem::remove(segment_dat, ec);
  std::filesystem::remove(schema_txt, ec);

  table->storage = std::make_unique<StorageEngine>();
  if (!table->open_storage()) {
    error = "failed to re-open table storage after delete";
    return false;
  }
  buffer_pool_.registerTable(table_id, table->storage.get());
  engine_.resetTableRuntime(table_id);
  if (!persistCatalog()) {
    error = "failed to persist catalog after delete";
    return false;
  }
  return true;
}

bool FlexQLServer::flushPendingRows(QueryResultContext &qctx) {
  if (qctx.pending_wire_buffer.empty()) {
    return true;
  }
  const bool ok = writeAll(qctx.client_fd, qctx.pending_wire_buffer.data(),
                           qctx.pending_wire_buffer.size());
  qctx.pending_wire_buffer.clear();
  if (!ok) {
    qctx.error = "failed to send row batch";
  }
  return ok;
}

bool FlexQLServer::rowCallback(void *ctx, const RowValue *projected_values,
                               size_t projected_count) {
  auto *qctx = static_cast<QueryResultContext *>(ctx);
  if (projected_count != qctx->projected_types.size()) {
    qctx->error = "projection type mismatch";
    return false;
  }

  // Compute payload size
  size_t payload_len = sizeof(uint16_t);
  for (size_t i = 0; i < projected_count; ++i) {
    const ColType t = qctx->projected_types[i];
    if (t == ColType::VARCHAR) {
      payload_len += 1 + sizeof(uint16_t) + projected_values[i].as_varchar.len;
    } else {
      payload_len += 1 + 8;
    }
  }

  // Write frame header + payload directly into pending_wire_buffer
  const size_t frame_size = 5 + payload_len;
  const size_t old_size = qctx->pending_wire_buffer.size();
  qctx->pending_wire_buffer.resize(old_size + frame_size);
  uint8_t *dest = qctx->pending_wire_buffer.data() + old_size;

  const uint32_t net_len = htonl(static_cast<uint32_t>(payload_len));
  std::memcpy(dest, &net_len, sizeof(uint32_t));
  dest[4] = static_cast<uint8_t>(MessageType::ROW);
  dest += 5;

  uint16_t col_count = static_cast<uint16_t>(projected_count);
  std::memcpy(dest, &col_count, sizeof(uint16_t));
  size_t cursor = sizeof(uint16_t);

  for (size_t i = 0; i < projected_count; ++i) {
    const ColType t = qctx->projected_types[i];
    dest[cursor++] = static_cast<uint8_t>(t);
    if (t == ColType::VARCHAR) {
      const uint16_t len = projected_values[i].as_varchar.len;
      std::memcpy(dest + cursor, &len, sizeof(uint16_t));
      cursor += sizeof(uint16_t);
      if (len > 0) {
        std::memcpy(dest + cursor, projected_values[i].as_varchar.buf, len);
        cursor += len;
      }
    } else if (t == ColType::DECIMAL) {
      std::memcpy(dest + cursor, &projected_values[i].as_double, 8);
      cursor += 8;
    } else if (t == ColType::DATETIME) {
      std::memcpy(dest + cursor, &projected_values[i].as_datetime, 8);
      cursor += 8;
    } else {
      std::memcpy(dest + cursor, &projected_values[i].as_int, 8);
      cursor += 8;
    }
  }

  if (qctx->pending_wire_buffer.size() >= kRowBatchFlushBytes &&
      !flushPendingRows(*qctx)) {
    return false;
  }
  qctx->rows_sent += 1;
  if ((qctx->rows_sent & 65535ULL) == 0) {
    if (!flushPendingRows(*qctx)) {
      return false;
    }
    if (consumeAbortFrameIfAvailable(qctx->client_fd)) {
      qctx->aborted = true;
      return false;
    }
  }
  return true;
}

bool FlexQLServer::handleSelect(const QueryAST &ast, int client_fd,
                                std::string &error) {
  QueryResultContext qctx{};
  qctx.client_fd = client_fd;
  qctx.aborted = false;
  qctx.rows_sent = 0;
  qctx.row_payload_buffer.reserve(1024);
  qctx.pending_wire_buffer.reserve(kRowBatchFlushBytes);

  Table *table = nullptr;
  uint32_t table_id = 0;
  if (!resolveTable(ast.table_name, table, table_id)) {
    error = "unknown table: " + ast.table_name;
    return false;
  }
  if (engine_.hasPendingWrites(table_id)) {
    if (!engine_.flushTable(*table, table_id)) {
      error = "failed to flush table before select";
      return false;
    }
  }
  // Lazy B+ tree rebuild: only for WHERE queries that need the index.
  // Full table scans bypass the B+ tree entirely.
  if (ast.where.has_value() && ast.where->col_index == 0 &&
      ast.where->op_code == CompareOp::EQ) {
    if (engine_.primaryKeyRuntimeEmpty(table_id) &&
        table->storage->pageCount() > 0) {
      engine_.warmupTableRuntimeFromDisk(*table, table_id);
    }
  }

  if (ast.join.has_value()) {
    Table *right = nullptr;
    uint32_t right_id = 0;
    if (!resolveTable(ast.join_table_name, right, right_id)) {
      error = "unknown join table: " + ast.join_table_name;
      return false;
    }
    if (engine_.hasPendingWrites(right_id)) {
      if (!engine_.flushTable(*right, right_id)) {
        error = "failed to flush join table before select";
        return false;
      }
    }
    qctx.projected_types.reserve(ast.join_projected.size());
    qctx.projected_names.reserve(ast.join_projected.size());
    for (const ProjectionRef &p : ast.join_projected) {
      if (p.table_side == TableSide::LEFT) {
        qctx.projected_types.push_back(
            table->schema.columns[static_cast<size_t>(p.col_index)].type);
        qctx.projected_names.push_back(
            table->name + "." +
            table->schema.columns[static_cast<size_t>(p.col_index)].name);
      } else {
        qctx.projected_types.push_back(
            right->schema.columns[static_cast<size_t>(p.col_index)].type);
        qctx.projected_names.push_back(
            right->name + "." +
            right->schema.columns[static_cast<size_t>(p.col_index)].name);
      }
    }
    if (!sendSchemaFrame(client_fd, qctx.projected_names)) {
      error = "failed to send schema";
      return false;
    }

    size_t matched_rows = 0;
    if (!engine_.executeJoin(ast, *table, table_id, *right, right_id,
                             &FlexQLServer::rowCallback, &qctx,
                             &matched_rows)) {
      if (qctx.aborted) {
        return true;
      }
      error = qctx.error.empty() ? "join execution failed" : qctx.error;
      return false;
    }
    if (!flushPendingRows(qctx)) {
      error = qctx.error.empty() ? "failed to flush row batch" : qctx.error;
      return false;
    }
    if (!qctx.aborted && !qctx.error.empty()) {
      error = qctx.error;
      return false;
    }
    return true;
  }

  qctx.projected_types.reserve(ast.projected_col_indices.size());
  qctx.projected_names.reserve(ast.projected_col_indices.size());
  for (int idx : ast.projected_col_indices) {
    qctx.projected_types.push_back(
        table->schema.columns[static_cast<size_t>(idx)].type);
    qctx.projected_names.push_back(
        table->schema.columns[static_cast<size_t>(idx)].name);
  }
  if (!sendSchemaFrame(client_fd, qctx.projected_names)) {
    error = "failed to send schema";
    return false;
  }

  size_t matched_rows = 0;
  if (ast.order_by_col_index.has_value()) {
    int projected_order_idx = -1;
    for (size_t i = 0; i < ast.projected_col_indices.size(); ++i) {
      if (ast.projected_col_indices[i] == *ast.order_by_col_index) {
        projected_order_idx = static_cast<int>(i);
        break;
      }
    }
    if (projected_order_idx < 0) {
      error = "ORDER BY column must be included in projection";
      return false;
    }

    CollectRowsContext collect{};
    collect.projected_count = ast.projected_col_indices.size();
    if (!engine_.execute(ast, *table, table_id, &collectRowsCallback, &collect,
                         &matched_rows)) {
      error = "select execution failed";
      return false;
    }

    const ColType order_type =
        table->schema.columns[static_cast<size_t>(*ast.order_by_col_index)]
            .type;
    std::sort(collect.rows.begin(), collect.rows.end(),
              [&](const std::vector<RowValue> &lhs,
                  const std::vector<RowValue> &rhs) {
                const int cmp = compareRowValue(
                    order_type, lhs[static_cast<size_t>(projected_order_idx)],
                    rhs[static_cast<size_t>(projected_order_idx)]);
                return ast.order_by_desc ? (cmp > 0) : (cmp < 0);
              });

    for (const auto &row : collect.rows) {
      if (!rowCallback(&qctx, row.data(), row.size())) {
        if (qctx.aborted) {
          return true;
        }
        error = qctx.error.empty() ? "select execution failed" : qctx.error;
        return false;
      }
    }
  } else if (!engine_.execute(ast, *table, table_id, &FlexQLServer::rowCallback,
                              &qctx, &matched_rows)) {
    if (qctx.aborted) {
      return true;
    }
    error = qctx.error.empty() ? "select execution failed" : qctx.error;
    return false;
  }
  if (!flushPendingRows(qctx)) {
    error = qctx.error.empty() ? "failed to flush row batch" : qctx.error;
    return false;
  }
  if (!qctx.aborted && !qctx.error.empty()) {
    error = qctx.error;
    return false;
  }
  return true;
}

bool FlexQLServer::execQuery(int client_fd, const std::string &sql) {
  std::string error;
  QueryAST ast{};
  const auto tables = snapshotTables();
  if (!parser_.parse(sql, tables, ast, error)) {
    return sendErrorFrame(client_fd, error);
  }

  bool ok = false;
  if (ast.type == QueryType::CREATE_TABLE) {
    ok = handleCreate(ast, error);
  } else if (ast.type == QueryType::INSERT) {
    ok = handleInsert(ast, error);
  } else if (ast.type == QueryType::DELETE) {
    ok = handleDelete(ast, error);
  } else if (ast.type == QueryType::SELECT) {
    ok = handleSelect(ast, client_fd, error);
  } else {
    error = "unsupported query type";
    ok = false;
  }

  if (!ok) {
    return sendErrorFrame(client_fd, error.empty() ? "query failed" : error);
  }
  return sendFrame(client_fd, MessageType::DONE, nullptr, 0);
}

bool FlexQLServer::persistCatalog() const {
  std::filesystem::create_directories("data");
  const std::string catalog_path = "data/catalog.txt";
  std::ofstream out(catalog_path, std::ios::trunc);
  if (!out.good()) {
    return false;
  }

  std::lock_guard<std::mutex> lock(catalog_mutex_);
  for (const auto &kv : table_ids_) {
    const std::string &table_name = kv.first;
    const uint32_t table_id = kv.second;
    Table *table = const_cast<Database &>(db_).getTable(table_name);
    if (table == nullptr) {
      continue;
    }
    out << table_name << "|" << table_id << "|";
    for (size_t i = 0; i < table->schema.columns.size(); ++i) {
      if (i > 0)
        out << ",";
      out << table->schema.columns[i].name << ":"
          << colTypeToString(table->schema.columns[i].type);
    }
    out << "\n";

    std::filesystem::create_directories("data/" + table_name);
    std::ofstream schema_out("data/" + table_name + "/schema.txt",
                             std::ios::trunc);
    if (schema_out.good()) {
      schema_out << "table=" << table_name << "\n";
      for (const auto &c : table->schema.columns) {
        schema_out << c.name << " " << colTypeToString(c.type) << "\n";
      }
    }
  }
  return out.good();
}

bool FlexQLServer::loadCatalog() {
  std::filesystem::create_directories("data");
  const std::string catalog_path = "data/catalog.txt";
  if (!std::filesystem::exists(catalog_path)) {
    return true;
  }

  std::ifstream in(catalog_path);
  if (!in.good()) {
    return false;
  }

  table_ids_.clear();
  uint32_t max_id = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty())
      continue;

    const size_t p1 = line.find('|');
    const size_t p2 =
        (p1 == std::string::npos) ? std::string::npos : line.find('|', p1 + 1);
    if (p1 == std::string::npos || p2 == std::string::npos) {
      continue;
    }
    const std::string table_name = line.substr(0, p1);
    const std::string id_str = line.substr(p1 + 1, p2 - p1 - 1);
    const std::string cols_str = line.substr(p2 + 1);

    uint32_t table_id = 0;
    try {
      table_id = static_cast<uint32_t>(std::stoul(id_str));
    } catch (...) {
      continue;
    }

    Schema schema;
    std::stringstream ss(cols_str);
    std::string col_tok;
    while (std::getline(ss, col_tok, ',')) {
      const size_t c = col_tok.find(':');
      if (c == std::string::npos)
        continue;
      const std::string col_name = col_tok.substr(0, c);
      const std::string col_type = col_tok.substr(c + 1);
      ColType t;
      if (!stringToColType(col_type, t))
        continue;
      schema.add_column(col_name, t);
    }
    if (schema.columns.empty()) {
      continue;
    }

    if (!db_.createTable(table_name, schema)) {
      continue;
    }
    Table *table = db_.getTable(table_name);
    if (table == nullptr) {
      continue;
    }
    {
      std::lock_guard<std::mutex> lock(catalog_mutex_);
      table_ids_[table_name] = table_id;
    }
    buffer_pool_.registerTable(table_id, table->storage.get());
    if (!engine_.warmupTableRuntimeFromDisk(*table, table_id)) {
      return false;
    }
    if (!engine_.recoverTable(*table, table_id)) {
      return false;
    }
    if (table_id > max_id) {
      max_id = table_id;
    }
  }
  next_table_id_.store(max_id + 1, std::memory_order_relaxed);
  return true;
}

void FlexQLServer::handleConnection(int client_fd) {
  while (isRunning()) {
    uint8_t header[5];
    if (!readAll(client_fd, header, sizeof(header))) {
      break;
    }

    uint32_t payload_len = 0;
    std::memcpy(&payload_len, header, sizeof(uint32_t));
    payload_len = ntohl(payload_len);
    const MessageType type = static_cast<MessageType>(header[4]);

    std::vector<uint8_t> payload;
    payload.resize(payload_len);
    if (payload_len > 0 && !readAll(client_fd, payload.data(), payload_len)) {
      break;
    }

    if (type == MessageType::QUIT) {
      break;
    }
    if (type == MessageType::ABORT) {
      continue;
    }
    if (type != MessageType::QUERY) {
      if (!sendErrorFrame(client_fd, "invalid message type")) {
        break;
      }
      continue;
    }

    const std::string sql(reinterpret_cast<const char *>(payload.data()),
                          payload.size());
    if (!execQuery(client_fd, sql)) {
      break;
    }
  }
  ::shutdown(client_fd, SHUT_RDWR);
  ::close(client_fd);
}

} // namespace flexql
