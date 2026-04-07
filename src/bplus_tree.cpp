#include "flexql/bplus_tree.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

namespace flexql {

namespace {

inline uint64_t rotl64(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

inline uint64_t round64(uint64_t acc, uint64_t input) {
  acc += input * 14029467366897019727ULL;
  acc = rotl64(acc, 31);
  acc *= 11400714785074694791ULL;
  return acc;
}

inline uint64_t mergeRound64(uint64_t acc, uint64_t val) {
  val = round64(0, val);
  acc ^= val;
  acc = acc * 11400714785074694791ULL + 9650029242287828579ULL;
  return acc;
}

inline uint64_t avalanche64(uint64_t h64) {
  h64 ^= h64 >> 33;
  h64 *= 14029467366897019727ULL;
  h64 ^= h64 >> 29;
  h64 *= 1609587929392839161ULL;
  h64 ^= h64 >> 32;
  return h64;
}

}  // namespace

uint64_t xxhash64(const void* data, size_t len, uint64_t seed) {
  const auto* p = static_cast<const uint8_t*>(data);
  const auto* end = p + len;
  uint64_t h64;

  if (len >= 32) {
    const auto* limit = end - 32;
    uint64_t v1 = seed + 11400714785074694791ULL + 14029467366897019727ULL;
    uint64_t v2 = seed + 14029467366897019727ULL;
    uint64_t v3 = seed + 0;
    uint64_t v4 = seed - 11400714785074694791ULL;

    do {
      uint64_t lane = 0;
      std::memcpy(&lane, p, sizeof(uint64_t));
      v1 = round64(v1, lane);
      std::memcpy(&lane, p + 8, sizeof(uint64_t));
      v2 = round64(v2, lane);
      std::memcpy(&lane, p + 16, sizeof(uint64_t));
      v3 = round64(v3, lane);
      std::memcpy(&lane, p + 24, sizeof(uint64_t));
      v4 = round64(v4, lane);
      p += 32;
    } while (p <= limit);

    h64 = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
    h64 = mergeRound64(h64, v1);
    h64 = mergeRound64(h64, v2);
    h64 = mergeRound64(h64, v3);
    h64 = mergeRound64(h64, v4);
  } else {
    h64 = seed + 2870177450012600261ULL;
  }

  h64 += static_cast<uint64_t>(len);

  while (p + 8 <= end) {
    uint64_t k1 = 0;
    std::memcpy(&k1, p, sizeof(uint64_t));
    k1 *= 14029467366897019727ULL;
    k1 = rotl64(k1, 31);
    k1 *= 11400714785074694791ULL;
    h64 ^= k1;
    h64 = rotl64(h64, 27) * 11400714785074694791ULL + 9650029242287828579ULL;
    p += 8;
  }

  if (p + 4 <= end) {
    uint32_t k1 = 0;
    std::memcpy(&k1, p, sizeof(uint32_t));
    h64 ^= static_cast<uint64_t>(k1) * 11400714785074694791ULL;
    h64 = rotl64(h64, 23) * 14029467366897019727ULL + 1609587929392839161ULL;
    p += 4;
  }

  while (p < end) {
    h64 ^= (*p) * 2870177450012600261ULL;
    h64 = rotl64(h64, 11) * 11400714785074694791ULL;
    ++p;
  }

  return avalanche64(h64);
}

uint64_t encodePrimaryKey(ColType type, const RowValue& value) {
  if (type == ColType::INT || type == ColType::DATETIME) {
    return static_cast<uint64_t>(value.as_int);
  }

  if (type == ColType::DECIMAL) {
    uint64_t bits = 0;
    std::memcpy(&bits, &value.as_double, sizeof(double));
    return bits;
  }

  return xxhash64(value.as_varchar.buf, value.as_varchar.len, 0);
}

BPlusTreeNode::BPlusTreeNode(bool leaf) : is_leaf(leaf), next_leaf(UINT32_MAX), keys(), values(), varchar_values(), children() {
  keys.reserve(BPlusTree::kMaxKeys + 1);
  if (leaf) {
    values.reserve(BPlusTree::kMaxKeys + 1);
  } else {
    children.reserve(BPlusTree::kOrder + 1);
  }
}

BPlusTree::BPlusTree() : root_(UINT32_MAX), node_pool_() {
  node_pool_.reserve(600000);
}

void BPlusTree::clear() {
  node_pool_.clear();
  root_ = UINT32_MAX;
}

bool BPlusTree::empty() const {
  return root_ == UINT32_MAX;
}

uint32_t BPlusTree::allocateNode(bool is_leaf) {
  node_pool_.emplace_back(is_leaf);
  return static_cast<uint32_t>(node_pool_.size() - 1);
}

bool BPlusTree::leafCollisionMatch(const BPlusTreeNode& node,
                                   size_t pos,
                                   const RowValue::VarcharValue* raw_varchar) const {
  if (raw_varchar == nullptr) {
    return true;
  }
  if (pos >= node.varchar_values.size()) {
    return false;
  }

  const auto& got = node.varchar_values[pos];
  if (got.len != raw_varchar->len) {
    return false;
  }
  return std::memcmp(got.buf, raw_varchar->buf, got.len) == 0;
}

BPlusTree::SplitResult BPlusTree::insertRecursive(uint32_t node_idx,
                                                  int64_t key,
                                                  RecordPointer ptr,
                                                  const RowValue::VarcharValue* raw_varchar) {
  BPlusTreeNode& node = node_pool_[node_idx];

  if (node.is_leaf) {
    // Fast-path: if key >= last key, append at end (common for sequential inserts).
    if (!node.keys.empty() && key >= node.keys.back()) {
      node.keys.push_back(key);
      node.values.push_back(ptr);
      if (raw_varchar != nullptr) {
        RowValue::VarcharValue vv{};
        vv.len = raw_varchar->len;
        if (vv.len > 0) {
          std::memcpy(vv.buf, raw_varchar->buf, vv.len);
        }
        if (vv.len < 254) {
          std::memset(vv.buf + vv.len, 0, static_cast<size_t>(254 - vv.len));
        }
        node.varchar_values.push_back(vv);
      }
    } else {
      const auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
      const size_t pos = static_cast<size_t>(it - node.keys.begin());

      node.keys.insert(it, key);
      node.values.insert(node.values.begin() + static_cast<std::ptrdiff_t>(pos), ptr);

      if (raw_varchar != nullptr) {
        RowValue::VarcharValue vv{};
        vv.len = raw_varchar->len;
        if (vv.len > 0) {
          std::memcpy(vv.buf, raw_varchar->buf, vv.len);
        }
        if (vv.len < 254) {
          std::memset(vv.buf + vv.len, 0, static_cast<size_t>(254 - vv.len));
        }
        node.varchar_values.insert(node.varchar_values.begin() + static_cast<std::ptrdiff_t>(pos), vv);
      }
    }

    if (node.keys.size() <= kMaxKeys) {
      return {false, 0, 0};
    }

    const size_t mid = node.keys.size() / 2;
    const uint32_t right_idx = allocateNode(true);
    BPlusTreeNode& right = node_pool_[right_idx];

    right.keys.assign(node.keys.begin() + static_cast<std::ptrdiff_t>(mid), node.keys.end());
    right.values.assign(node.values.begin() + static_cast<std::ptrdiff_t>(mid), node.values.end());
    if (!node.varchar_values.empty()) {
      right.varchar_values.assign(node.varchar_values.begin() + static_cast<std::ptrdiff_t>(mid),
                                  node.varchar_values.end());
      node.varchar_values.resize(mid);
    }

    node.keys.resize(mid);
    node.values.resize(mid);

    right.next_leaf = node.next_leaf;
    node.next_leaf = right_idx;

    return {true, right.keys.front(), right_idx};
  }

  size_t child_pos = 0;
  while (child_pos < node.keys.size() && key >= node.keys[child_pos]) {
    ++child_pos;
  }

  SplitResult child_split = insertRecursive(node.children[child_pos], key, ptr, raw_varchar);
  if (!child_split.has_split) {
    return {false, 0, 0};
  }

  node.keys.insert(node.keys.begin() + static_cast<std::ptrdiff_t>(child_pos), child_split.promoted_key);
  node.children.insert(node.children.begin() + static_cast<std::ptrdiff_t>(child_pos + 1),
                       child_split.right_node);

  if (node.keys.size() <= kMaxKeys) {
    return {false, 0, 0};
  }

  const size_t mid = node.keys.size() / 2;
  const int64_t promote = node.keys[mid];

  const uint32_t right_idx = allocateNode(false);
  BPlusTreeNode& right = node_pool_[right_idx];

  right.keys.assign(node.keys.begin() + static_cast<std::ptrdiff_t>(mid + 1), node.keys.end());
  right.children.assign(node.children.begin() + static_cast<std::ptrdiff_t>(mid + 1), node.children.end());

  node.keys.resize(mid);
  node.children.resize(mid + 1);

  return {true, promote, right_idx};
}

bool BPlusTree::insert(int64_t key, RecordPointer ptr, const RowValue::VarcharValue* raw_varchar) {
  if (root_ == UINT32_MAX) {
    root_ = allocateNode(true);
    BPlusTreeNode& root = node_pool_[root_];
    root.keys.push_back(key);
    root.values.push_back(ptr);

    if (raw_varchar != nullptr) {
      RowValue::VarcharValue vv{};
      vv.len = raw_varchar->len;
      if (vv.len > 0) {
        std::memcpy(vv.buf, raw_varchar->buf, vv.len);
      }
      if (vv.len < 254) {
        std::memset(vv.buf + vv.len, 0, static_cast<size_t>(254 - vv.len));
      }
      root.varchar_values.push_back(vv);
    }
    return true;
  }

  SplitResult split = insertRecursive(root_, key, ptr, raw_varchar);
  if (!split.has_split) {
    return true;
  }

  const uint32_t new_root = allocateNode(false);
  BPlusTreeNode& r = node_pool_[new_root];
  r.keys.push_back(split.promoted_key);
  r.children.push_back(root_);
  r.children.push_back(split.right_node);
  root_ = new_root;
  return true;
}

bool BPlusTree::search(int64_t key, RecordPointer& out_ptr, const RowValue::VarcharValue* raw_varchar) const {
  if (root_ == UINT32_MAX) {
    return false;
  }

  uint32_t node_idx = root_;
  while (true) {
    const BPlusTreeNode& node = node_pool_[node_idx];
    if (node.is_leaf) {
      auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
      while (it != node.keys.end() && *it == key) {
        const size_t pos = static_cast<size_t>(it - node.keys.begin());
        if (leafCollisionMatch(node, pos, raw_varchar)) {
          out_ptr = node.values[pos];
          return true;
        }
        ++it;
      }
      return false;
    }

    size_t child_pos = 0;
    while (child_pos < node.keys.size() && key >= node.keys[child_pos]) {
      ++child_pos;
    }
    node_idx = node.children[child_pos];
  }
}

void BPlusTree::buildBulkFromSorted(const std::vector<std::pair<int64_t, RecordPointer>>& sorted_pairs) {
  clear();
  if (sorted_pairs.empty()) {
    return;
  }

  std::vector<uint32_t> level;
  level.reserve((sorted_pairs.size() / kMaxKeys) + 2);

  size_t i = 0;
  while (i < sorted_pairs.size()) {
    const uint32_t leaf_idx = allocateNode(true);
    BPlusTreeNode& leaf = node_pool_[leaf_idx];

    size_t take = std::min(kMaxKeys, sorted_pairs.size() - i);
    for (size_t j = 0; j < take; ++j) {
      leaf.keys.push_back(sorted_pairs[i + j].first);
      leaf.values.push_back(sorted_pairs[i + j].second);
      RowValue::VarcharValue empty{};
      empty.len = 0;
      std::memset(empty.buf, 0, sizeof(empty.buf));
      leaf.varchar_values.push_back(empty);
    }

    if (!level.empty()) {
      node_pool_[level.back()].next_leaf = leaf_idx;
    }
    level.push_back(leaf_idx);
    i += take;
  }

  while (level.size() > 1) {
    std::vector<uint32_t> parent_level;
    parent_level.reserve((level.size() / kOrder) + 2);

    size_t cursor = 0;
    while (cursor < level.size()) {
      const uint32_t parent_idx = allocateNode(false);
      BPlusTreeNode& parent = node_pool_[parent_idx];

      size_t children_take = std::min(kOrder, level.size() - cursor);
      for (size_t c = 0; c < children_take; ++c) {
        const uint32_t child_idx = level[cursor + c];
        parent.children.push_back(child_idx);
        if (c > 0) {
          parent.keys.push_back(node_pool_[child_idx].keys.front());
        }
      }

      parent_level.push_back(parent_idx);
      cursor += children_take;
    }

    level.swap(parent_level);
  }

  root_ = level.front();
}

}  // namespace flexql
