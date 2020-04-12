//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>  // NOLINT
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : page_number((num_buckets - 1) / buckets_pro_page + 1), buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // get a header page from the BufferPoolManager
  auto header_page = reinterpret_cast<HashTableHeaderPage *>
                       (buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
  header_page->SetSize(num_buckets);
  size_cache = num_buckets;
  header_page->SetPageId(header_page_id_);
  header_page->SetLSN(0);
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
  // allocate block pages for the bucket number
  page_ids_cache.reserve(page_number);
  for (size_t i = 0; i < page_number; i++) {
    buffer_pool_manager_->NewPage(&page_ids_cache[i]);
    buffer_pool_manager_->UnpinPage(page_ids_cache[i], false);
  }
  // add block page id to the header page
  header_page = reinterpret_cast<HashTableHeaderPage *>
                  (buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
  for (size_t i = 0; i < page_number; i++) {
    header_page->AddBlockPageId(page_ids_cache[i]);
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  const uint64_t hash_value = hash_fn_.GetHash(key);
  const size_t hash_position = hash_value % size_cache;
  const auto page_postion = GetPagePosition(hash_position);
  size_t page_index = page_postion.first;
  slot_offset_t slot_offset = page_postion.second;
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                      (buffer_pool_manager_->FetchPage(page_ids_cache[page_index])->GetData());
  while (block_page->IsOccupied(slot_offset)) {
    if (block_page->IsReadable(slot_offset) && comparator_(key, block_page->KeyAt(slot_offset)) == 0) {
      result->emplace_back(block_page->ValueAt(slot_offset));
    }
    // SAME SAME SAME
    if (++slot_offset == PAGE_SIZE) {
      slot_offset = 0;
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], true);
      if (++page_index == page_number) {
        page_index = 0;
      }
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                     (buffer_pool_manager_->FetchPage(page_ids_cache[page_index])->GetData());
    }
  }
  buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
  return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  const uint64_t hash_value = hash_fn_.GetHash(key);
  const size_t hash_position = hash_value % size_cache;
  const auto page_postion = GetPagePosition(hash_position);
  size_t page_index = page_postion.first;
  slot_offset_t slot_offset = page_postion.second;
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                      (buffer_pool_manager_->FetchPage(page_ids_cache[page_index])->GetData());
  while (!block_page->Insert(slot_offset, key, value)) {
    if (comparator_(key, block_page->KeyAt(slot_offset)) == 0 && value == block_page->ValueAt(slot_offset)) {
      return false;
    }
    // SAME SAME SAME
    if (++slot_offset == PAGE_SIZE) {
      slot_offset = 0;
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], true);
      if (++page_index == page_number) {
        page_index = 0;
      }
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                     (buffer_pool_manager_->FetchPage(page_ids_cache[page_index])->GetData());
    }
  }
  buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  const uint64_t hash_value = hash_fn_.GetHash(key);
  const size_t hash_position = hash_value % size_cache;
  const auto page_postion = GetPagePosition(hash_position);
  size_t page_index = page_postion.first;
  slot_offset_t slot_offset = page_postion.second;
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                      (buffer_pool_manager_->FetchPage(page_ids_cache[page_index])->GetData());
  while (block_page->IsOccupied(slot_offset)) {
    if (block_page->IsReadable(slot_offset)
        && comparator_(key, block_page->KeyAt(slot_offset)) == 0
        && value == block_page->ValueAt(slot_offset)) {
      block_page->Remove(slot_offset);
      return true;
    }
    // SAME SAME SAME
    if (++slot_offset == PAGE_SIZE) {
      slot_offset = 0;
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], true);
      if (++page_index == page_number) {
        page_index = 0;
      }
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                     (buffer_pool_manager_->FetchPage(page_ids_cache[page_index])->GetData());
    }
  }
  buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], true);
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  const auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));
  const auto size = header_page->GetSize();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  assert(size == size_cache);
  return size_cache;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
