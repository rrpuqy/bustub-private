//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// trie.cpp
//
// Identification: src/primer/trie.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/trie.h"
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

/**
 * @brief Get the value associated with the given key.
 * 1. If the key is not in the trie, return nullptr.
 * 2. If the key is in the trie but the type is mismatched, return nullptr.
 * 3. Otherwise, return the value.
 */
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if(root_ == nullptr)  {
    return nullptr;
  }
  auto start = root_;
  for(char c:key) {
     auto it = start->children_.find(c);
     if(it == start->children_.end()) {
      return nullptr;
     }
     start = it->second;
  }
  auto node = dynamic_cast<const TrieNodeWithValue<T>*>(start.get());
  if(node == nullptr) {
    return nullptr;
  }
  return node->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::PutInternal(std::unique_ptr<TrieNode> new_node,std::string_view key, T value) const -> std::shared_ptr<const TrieNode> {
  
  // auto new_node = pre->Clone();
  auto it = new_node->children_.find(key[0]);
  if(key.size() == 1) {
    if(it == new_node->children_.end()) {
      new_node->children_.insert({key[0], std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)))});
      return std::shared_ptr<TrieNode>(std::move(new_node));
    }
    auto childnode = it->second;
    auto new_node_with_value = std::make_shared<TrieNodeWithValue<T>>(childnode->children_,std::make_shared<T>(std::move(value)));
    new_node->children_[key[0]] = std::move(new_node_with_value);
    return std::shared_ptr<TrieNode>(std::move(new_node));  
  }

  if(it == new_node->children_.end()) {
    new_node->children_.insert({key[0], PutInternal(std::make_unique<TrieNode>(), key.substr(1), std::move(value))});
    
  }
  else {
    auto childnode = it->second->Clone();
    new_node->children_[key[0]] = PutInternal(std::move(childnode), key.substr(1), std::move(value));
  }
  return std::shared_ptr<TrieNode>(std::move(new_node));

}

/**
 * @brief Put a new key-value pair into the trie. If the key already exists, overwrite the value.
 * @return the new trie.
 */
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // if(key.empty() ) {
  //   return *this;
  // }
  if(key.empty()) {
    return Trie(std::make_shared<TrieNodeWithValue<T>>(root_->children_,std::make_shared<T>(std::move(value))));
  }

  auto new_root = (root_ == nullptr) ? (std::make_unique<TrieNode>(TrieNode())): root_->Clone();
  // if(root_ == nullptr){
  //   root_ = std::make_shared<TrieNode>();
  // }
  
  return Trie(std::move(PutInternal(std::move(new_root), key, std::move(value))));
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::RemoveInternal(std::unique_ptr< TrieNode> pre,std::string_view key) const -> std::unique_ptr<TrieNode>{
  auto it = pre->children_.find(key[0]);

  if(it == pre->children_.end()) {
    return pre;
  }
  if(key.size() == 1) {
    if(it->second->children_.empty()) {
      pre->children_.erase(it);
      return pre;
    }
    // auto new_node = TrieNode(it->second->children_);
    pre->children_[key[0]] =(std::make_shared<TrieNode>(it->second->children_));
    return pre;
  }

  auto childnode = it->second->Clone();
  childnode = RemoveInternal(std::move(childnode), key.substr(1));
  // pre->children_[key[0]] = std::move(childnode);
  if(childnode->children_.empty() && !childnode->is_value_node_) {
    pre->children_.erase(it);
  }
  else{
    pre->children_[key[0]] = std::move(childnode);
  }
  return pre;
  
}
/**
 * @brief Remove the key from the trie.
 * @return If the key does not exist, return the original trie. Otherwise, returns the new trie.
 */
auto Trie::Remove(std::string_view key) const -> Trie {
  if(root_ == nullptr) {
    return *this;
  }
  auto new_root = key.empty()?std::make_unique<TrieNode>(TrieNode(root_->children_)):root_->Clone();

  // auto new_root_ptr = std::make_shared<TrieNode>(std::move(new_root));
  auto new_root_unique = RemoveInternal(std::move(new_root), key);
  if(new_root_unique->children_.empty() && !new_root_unique->is_value_node_) {
    return {};
  }
  return Trie(std::move(new_root_unique));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
