//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "fmt/chrono.h"
namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  explicit LRUKNode(frame_id_t fid, size_t k) : k_(k), fid_(fid) {}
  auto GetBackwardKDistance() const -> size_t {
    if (history_.size() < k_) {
      // return std::numeric_limits<size_t>::max();
      return 0;
    }
    return history_.front();
  }
  void RecordAccess(size_t timestamp) {
    if (history_.size() == k_) {
      history_.pop_front();
    }
    history_.push_back(timestamp);
  }

  void SetEvictable(bool set_evictable) { is_evictable_ = set_evictable; }

  auto IsEvictable() const -> bool { return is_evictable_; }

  auto GetHistroy() const -> std::list<size_t> { return history_; }
  auto operator<(const LRUKNode &other) -> bool {
    if (history_.size() < k_ && other.history_.size() < k_) {
      return history_.back() < other.history_.back();
    }
    return GetBackwardKDistance() < other.GetBackwardKDistance();
  }

 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  [[maybe_unused]] std::list<size_t> history_;
  [[maybe_unused]] size_t k_;
  [[maybe_unused]] frame_id_t fid_;
  [[maybe_unused]] bool is_evictable_{false};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  [[maybe_unused]] std::unordered_map<frame_id_t, std::pair<LRUKNode, std::list<frame_id_t>::iterator> > node_store_;
  [[maybe_unused]] std::list<frame_id_t> lru_list_;
  [[maybe_unused]] size_t current_timestamp_{0};
  [[maybe_unused]] size_t curr_size_{0};
  [[maybe_unused]] size_t replacer_size_;
  [[maybe_unused]] size_t k_;
  [[maybe_unused]] std::mutex latch_;
};

}  // namespace bustub
