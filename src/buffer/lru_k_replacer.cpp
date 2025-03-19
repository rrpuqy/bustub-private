//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iterator>
#include <optional>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/chrono.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
    std::scoped_lock<std::mutex> lock(latch_);
    if(curr_size_ == 0){
        return std::nullopt;
    }
    auto it = lru_list_.begin();
    // 从前往后遍历 debug 打印链表值
    std::cout<< "lru_list_: ";
    for(int & it : lru_list_) {
        std::cout << it << " ";
        // 输出访问历史
        auto node = node_store_.find(it);
        std::cout << "history: ";
        for(auto & timestamp : node->second.first.GetHistroy()) {
            std::cout << timestamp << " ";
        }
    }
    std::cout << std::endl;

    while(it!=lru_list_.end()){
        auto node = node_store_.find(*it);
        if(node->second.first.IsEvictable()){
            auto frame_id = *it;
            lru_list_.erase(it);
            node_store_.erase(frame_id);
            --curr_size_;
            return {frame_id};
        }
        it++;
    }
    return std::nullopt;
 }

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    // 修复后的参数检查
    if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "frame_id is invalid");
    }
    std::scoped_lock<std::mutex> lock(latch_);
    auto now = std::chrono::system_clock::now();
    auto timestamp = fmt::detail::to_time_t(now);
    auto node_iter = node_store_.find(frame_id);
    if(node_iter == node_store_.end()){
        LRUKNode new_node(frame_id, k_);
        new_node.RecordAccess(timestamp);
        auto iter=lru_list_.begin();
        for(;iter!=lru_list_.end();iter++){
            auto node = node_store_.find(*iter)->second.first;
            if(new_node < node){
                lru_list_.insert(iter,frame_id);
                break;
            }
        }
        // node_store_.emplace(frame_id,new_node,lru_list_.begin());  // 修改这里
        if(iter == lru_list_.end()){
            lru_list_.push_back(frame_id);
        }
        node_store_.emplace(frame_id, std::make_pair(new_node, std::prev(iter)));
        // ++curr_size_;
    }
    else{
        auto &lru_node = node_iter->second;
        lru_node.first.RecordAccess(timestamp);
        // 从后往前遍历
        auto list_itr = lru_node.second;
        auto cur_iter = std::next(list_itr);
        lru_list_.erase(list_itr);
        while(cur_iter != lru_list_.end()){
            auto next_node = node_store_.find(*cur_iter)->second.first;
            if((lru_node.first < next_node)){
                lru_list_.insert(cur_iter, frame_id);
                lru_node.second = std::prev(cur_iter);
                break;            
            }
            ++cur_iter;
        }
        if(cur_iter == lru_list_.end()){
            lru_list_.push_back(frame_id);
            lru_node.second = std::prev(cur_iter);
        }
        // lru_list_.erase(it);
        // lru_list_.push_back(frame_id)
       
        // while(!lru_list_.empty() && !node_store_[lru_list_.front()].IsEvictable()){
            
        // }
    }
        // 从前往后遍历 debug 打印链表值
    std::cout<< "lru_list_: ";
    for(int & it : lru_list_) {
        std::cout << it << " ";
        // 输出访问历史
        auto node = node_store_.find(it);
        std::cout << "history: ";
        for(auto & timestamp : node->second.first.GetHistroy()) {
            std::cout << timestamp << " ";
        }
    }
    std::cout << std::endl;

}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    auto node = node_store_.find(frame_id);
    if(node == node_store_.end()){
        return;
    }
    if(node->second.first.IsEvictable() == set_evictable){
        return;
    }
    if(set_evictable){
        node->second.first.SetEvictable(true);
        curr_size_++;
    }
    else{
        node->second.first.SetEvictable(false);
        curr_size_--;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if(node_store_.find(frame_id)==node_store_.end()) {
        return;
    }
    if(!node_store_.find(frame_id)->second.first.IsEvictable()){
       throw Exception("frame is not evictable");
    }
    lru_list_.erase(node_store_.find(frame_id)->second.second);
    node_store_.erase(frame_id);
    --curr_size_;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
