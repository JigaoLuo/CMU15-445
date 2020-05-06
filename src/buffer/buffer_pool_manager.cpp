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
#include "common/logger.h"  // NOLINT

#include <list>  // NOLINT
#include <unordered_map>  // NOLINT

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
  //  Project4 require Buffer Pool Manager not to flush all dirty pages.
  //  FlushAllPagesImpl();  // Add by Jigao
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  std::unique_lock u_lock(global_latch_);
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
  return Evict(page_id, false, &u_lock);
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  assert(page_id != INVALID_PAGE_ID);
  std::unique_lock u_lock(global_latch_);
  // 1. search page table.
  const auto& got = page_table_.find(page_id);
  assert(got != page_table_.end());
  auto page = pages_ + got->second;
  // 2. if pin_count <= 0 before this call, return false
  if (page->pin_count_ <= 0) {
    return false;
  }
  // 3. if pin_count > 0, decrement it and if it becomes zero, put it back to replacer
  if (--page->pin_count_ == 0) {
    replacer_->Unpin(got->second);
  }
  // 4. is_dirty: set the dirty flag of this page
  page->is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  // Make sure you call DiskManager::WritePage!
  std::shared_lock s_lock(global_latch_);
  // 1. search page table.
  const auto& got = page_table_.find(page_id);
  if (got == page_table_.end()) {
    // 1.1. if page is not found in page table, return false
    s_lock.unlock();
    return false;
  }
  // 1.2. if page is not found in page table and dirty, call the write_page method of the disk manager
  const auto page = pages_ + got->second;
  page->WLatch();
  s_lock.unlock();
  if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
    page->is_dirty_ = false;
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page->WUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::unique_lock u_lock(global_latch_);
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0) {
    assert(IsAllPinned());
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  // 2.   call disk manager to allocate a page
  page_id_t new_page_id = disk_manager_->AllocatePage();
  // 3.   Pick a victim page
  Page *const page = Evict(new_page_id, true, &u_lock);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page_id;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  std::unique_lock u_lock(global_latch_);
  // 1.   Search the page table for the requested page (P).
  const auto& got = page_table_.find(page_id);
  if (got == page_table_.end()) {
    // 1.   If P does not exist, return true.
    u_lock.unlock();
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  const auto offset = got->second;
  Page *const page = pages_ + offset;
  page->WLatch();
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->pin_count_ != 0) {
    u_lock.unlock();
    page->WUnlatch();
    return false;
  }
  // 3.   Otherwise, P can be deleted.
  // Remove P from the page table, reset its metadata and return it to the free list.
  replacer_->Pin(offset);  // Remove from replacer, since the pin count is 0
  page_table_.erase(got);
  free_list_.emplace_back(offset);
  u_lock.unlock();

  disk_manager_->DeallocatePage(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->WUnlatch();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::shared_mutex> lock(global_latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    const auto page = pages_ + i;
    if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

Page *BufferPoolManager::Evict(page_id_t page_id, bool new_page, std::unique_lock<std::shared_mutex>* u_lock) {
  frame_id_t frame_r_id;
  Page *page = nullptr;
  // 0      For Project4
  //       > In your BufferPoolManager, when a new page is created,
  //       > if there is already an entry in the page_table_ mapping for the given page id,
  //       > you should make sure you explicitly overwrite it with the frame id of the new page that was just created.
//  if (new_page) {
//    auto it = page_table_.find(page_id);
//    if (it != page_table_.end()) {
//      frame_r_id = it->second;
//      page = pages_ + frame_r_id;
//      page->WLatch();
//      u_lock->unlock();
//      if (page->is_dirty_) {
//        int iii =5l;
//        iii++;
//      }
////      page->is_dirty_ = false;
//      goto last;
//    }
//  }
  // 1      If P does not exist, find a replacement page (R) from either the free list or the replacer.
  assert(!free_list_.empty() || replacer_->Size() != 0);
  if (!free_list_.empty()) {
    // 2.1     always find from free list first
    frame_r_id = free_list_.front();
    free_list_.pop_front();
    page_table_.emplace(page_id, frame_r_id);
    page = pages_ + frame_r_id;
    page->WLatch();
    u_lock->unlock();
    assert(page->pin_count_ == 0);
    assert(!page->is_dirty_);
    assert(page->page_id_ == INVALID_PAGE_ID);
    // 2.1.1     If not called by NewPage, then have to read the page into frame
    if (!new_page) {
      disk_manager_->ReadPage(page_id, page->data_);
    }
  } else {
    // 2.2. then find from replacer
    [[maybe_unused]] const bool victim_res = replacer_->Victim(&frame_r_id);
    assert(victim_res);
    page = pages_ + frame_r_id;
    // 2.2.1.     Delete R from the page table and insert P.
    page_table_.erase(page->page_id_);
    page_table_.emplace(page_id, frame_r_id);
    replacer_->Pin(frame_r_id);
    page->WLatch();
    u_lock->unlock();
    assert(page->pin_count_ == 0);
    assert(page->page_id_ != INVALID_PAGE_ID);
    // 2.2.2.     If R is dirty, write it back to the disk.
    if (page->is_dirty_) {
      // Project 4.
      // Before your buffer pool manager evicts a dirty page from LRU replacer and write this page back to db file,
      // it needs to flush logs up to pageLSN. You need to compare persistent_lsn_ (a member variable maintains
      // by Log Manager) with your pageLSN. However unlike group commit, buffer pool can force log manager to flush log
      // buffer, but still needs to wait for logs to be permanently stored before continue
      if (new_page) {
        if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
            LOG_INFO("BufferPoolManager::Evict := Evict a Dirty Page, Triggering a Flushing Log to Disk.");  // NOLINT
            log_manager_->Flush(true);
        }
      }
      disk_manager_->WritePage(page->page_id_, page->data_);
//      page->is_dirty_ = false;
    }
    // 2.2.3.     If not called by NewPage, read in the page content from disk
    if (!new_page) {
      disk_manager_->ReadPage(page_id, page->data_);
    } else {
      page->ResetMemory();
    }
  }
//  last:
  // 3.     Update P's metadata and then return a pointer to P.
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  // For Project 4 := new page is assumed always dirty, since the unpin can't be called at DBMS-Down-Time.
  page->is_dirty_ = new_page;
  page->WUnlatch();
  return page;
}
}  // namespace bustub
