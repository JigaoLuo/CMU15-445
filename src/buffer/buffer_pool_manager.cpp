//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  const auto& frame_id = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if (frame_id != page_table_.end())
    return pages_ + frame_id->second;
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.

  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  auto const& frame_id = page_table_[page_id];
  auto& page = pages_[frame_id];
  page.is_dirty_ = is_dirty;
  if (page.pin_count_ <= 0) return false;

  replacer_->Unpin(frame_id);
  page.pin_count_--;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  page_id_t new_page_id = disk_manager_->AllocatePage();

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  //TODO:

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  // 2.1  Pick from the free list first.
  if (free_list_.size() != 0) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 2.2 Pick from clock.
    // If no victim (buffer pool is full and all pages being used), return nullptr.
    if(!replacer_->Victim(&frame_id)) return nullptr;
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  auto& p = pages_[frame_id];

  //TODO: latch

  p.page_id_ = new_page_id;
  p.pin_count_ = 0;
  p.is_dirty_ = false;
  p.ResetMemory();

  // TODO: latch
  page_table_.emplace(new_page_id, frame_id);

  // PIN THIS PAGE
  replacer_->Pin(frame_id);
  p.pin_count_++;


  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page_id;
  return &p;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
