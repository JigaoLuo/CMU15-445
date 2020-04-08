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
  FlushAllPagesImpl();  // Add by Jigao
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1.     Search the page table for the requested page (P).
  const auto& got = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if (got != page_table_.end()) {
    Page *const page = pages_ + got->second;
    if (page->pin_count_++ == 0) {
      replacer_->Pin(got->second);
    }
    return page;
  }
  // 2.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0) {
    assert(IsAllPinned());
    return nullptr;
  }
  // 3.   Pick a victim page
  return Evict(page_id, false);
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  assert(page_id != INVALID_PAGE_ID);
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1. search page table.
  const auto& got = page_table_.find(page_id);
  assert(got != page_table_.end());
  auto page = pages_ + got->second;
  // 2. if pin_count <= 0 before this call, return false
  if (page->pin_count_ <= 0) return false;  // NOLINT
  // 3. is_dirty: set the dirty flag of this page
  page->is_dirty_ = is_dirty;
  // 4. if pin_count > 0, decrement it and if it becomes zero, put it back to replacer
  if (--page->pin_count_ == 0) {
    replacer_->Unpin(got->second);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1. search page table.
  const auto& got = page_table_.find(page_id);
  if (got == page_table_.end()) {
    // 1.1. if page is not found in page table, return false
    return false;
  }
  // 1.2. if page is not found in page table and dirty, call the write_page method of the disk manager
  const auto page = pages_ + got->second;
  if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0) {
    assert(IsAllPinned());
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  // 2.   call disk manager to allocate a page
  page_id_t new_page_id = disk_manager_->AllocatePage();
  // 3.   Pick a victim page
  Page *const page = Evict(new_page_id, true);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page_id;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1.   Search the page table for the requested page (P).
  const auto& got = page_table_.find(page_id);
  if (got == page_table_.end()) {
    // 1.   If P does not exist, return true.
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  const auto offset = got->second;
  Page *const page = pages_ + offset;
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->pin_count_ != 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted.
  // Remove P from the page table, reset its metadata and return it to the free list.
  replacer_->Pin(offset);  // Remove from replacer, since the pin count is 0
  page_table_.erase(got);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  free_list_.emplace_back(offset);
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::shared_mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    const auto page = pages_ + i;
    if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

Page *BufferPoolManager::Evict(page_id_t page_id, bool new_page) {
  // 1      If P does not exist, find a replacement page (R) from either the free list or the replacer.
  assert(!free_list_.empty() || replacer_->Size() != 0);
  frame_id_t frame_r_id;
  Page * page;
  if (!free_list_.empty()) {
    // 2. always find from free list first
    frame_r_id = free_list_.front();
    page = pages_ + frame_r_id;
    assert(page->pin_count_ == 0);
    assert(!page->is_dirty_);
    assert(page->page_id_ == INVALID_PAGE_ID);
    free_list_.pop_front();
    // 2.1 If not called by NewPage, then have to read the page into frame
    if (!new_page) {
      disk_manager_->ReadPage(page_id, page->data_);
    }
  } else {
    // 3. then find from replacer
    const bool victim_res = replacer_->Victim(&frame_r_id);
    assert(victim_res);
    page = pages_ + frame_r_id;
    assert(page->pin_count_ == 0);
    assert(page->page_id_ != INVALID_PAGE_ID);
    // 3.1     If R is dirty, write it back to the disk.
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    // 4.     Delete R from the page table and insert P.
    page_table_.erase(page->page_id_);
    replacer_->Pin(frame_r_id);
    // 5.     If not called by NewPage, read in the page content from disk
    if (!new_page) {
      disk_manager_->ReadPage(page_id, page->data_);
    } else {
      page->ResetMemory();
    }
  }
  // 6.     Update P's metadata and then return a pointer to P.
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page_table_.emplace(page->page_id_, frame_r_id);
  return page;
}
}  // namespace bustub
