//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  request_queue_.Put(std::move(r));
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  // TODO(P1): remove this line after you have implemented the disk scheduler API

  // 从队列中获取请求并处理
  while (true) {
    // 从队列中获取请求
    auto request = request_queue_.Get();
    // 如果请求为空，则退出循环
    if (!request.has_value()) {
      break;
    }
    // 处理请求

    if (request->is_write_) {
      // 写入请求
      disk_manager_->WritePage(request->page_id_, request->data_);
      // 调用回调函数
      request->callback_.set_value(true);

    } else {
      // 读取请求
      disk_manager_->ReadPage(request->page_id_, request->data_);
      // 调用回调函数
      request->callback_.set_value(true);
    }
  }
}

}  // namespace bustub
