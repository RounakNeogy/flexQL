#include "flexql/database.hpp"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "flexql/lock_order.hpp"

namespace flexql {

bool Database::createTable(std::string table_name, const Schema& schema) {
  DebugLockLevelGuard level_guard(LockLevel::TABLE_MAP);
  std::unique_lock<std::shared_mutex> lock(table_map_mutex);
  if (tables_.find(table_name) != tables_.end()) {
    return false;
  }

  auto table = std::make_unique<Table>(table_name, schema.columns.size());
  table->schema = schema;
  if (!table->open_storage()) {
    return false;
  }
  tables_.emplace(std::move(table_name), std::move(table));
  return true;
}

Table* Database::getTable(const std::string& table_name) {
  DebugLockLevelGuard level_guard(LockLevel::TABLE_MAP);
  std::shared_lock<std::shared_mutex> lock(table_map_mutex);
  const auto it = tables_.find(table_name);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.get();
}

}  // namespace flexql

