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
    : page_number((num_buckets - 1) / BLOCK_ARRAY_SIZE_PRO_PAGE + 1), buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // get a header page from the BufferPoolManager
  auto bpm_head_page = buffer_pool_manager_->NewPage(&header_page_id_);
  bpm_head_page->WLatch();
  auto ht_header_page = reinterpret_cast<HashTableHeaderPage *>(bpm_head_page->GetData());
  ht_header_page->SetSize(num_buckets);
  size_cache = num_buckets;
  BLOCK_ARRAY_SIZE_LAST_PAGE = num_buckets - BLOCK_ARRAY_SIZE_PRO_PAGE * (page_number - 1);
  ht_header_page->SetPageId(header_page_id_);
  ht_header_page->SetLSN(0);
  bpm_head_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);

  // allocate block pages for the bucket number
  page_ids_cache.reserve(page_number);
  page_id_t page_id;
  for (size_t i = 0; i < page_number; i++) {
    buffer_pool_manager_->NewPage(&page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_ids_cache.emplace_back(page_id);
  }

  // add block page id to the header page
  bpm_head_page = buffer_pool_manager_->FetchPage(header_page_id_);
  bpm_head_page->WLatch();
  ht_header_page = reinterpret_cast<HashTableHeaderPage *>(bpm_head_page->GetData());
  for (size_t i = 0; i < page_number; i++) {
    ht_header_page->AddBlockPageId(page_ids_cache[i]);
  }
  bpm_head_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
  // TODO(jigao): cold start and hot sstart 进行区分
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  const uint64_t hash_value = hash_fn_.GetHash(key);
  table_latch_.RLock();
  const size_t hash_position = hash_value % size_cache;
  const auto page_postion = GetPagePosition(hash_position);
  const size_t page_index_start = page_postion.first;
  const slot_offset_t slot_offset_start = page_postion.second;
  size_t page_index = page_postion.first;
  slot_offset_t slot_offset = page_postion.second;
  auto bpm_page = buffer_pool_manager_->FetchPage(page_ids_cache[page_index]);
  bpm_page->RLatch();
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(bpm_page->GetData());
  while (block_page->IsOccupied(slot_offset)) {
    if (block_page->IsReadable(slot_offset)
        && comparator_(key, block_page->KeyAt(slot_offset)) == 0) {
      result->emplace_back(block_page->ValueAt(slot_offset));
    }
    // SAME SAME SAME
    if (++slot_offset == ((page_index == page_number - 1) ? BLOCK_ARRAY_SIZE_LAST_PAGE : BLOCK_ARRAY_SIZE_PRO_PAGE)) {
      slot_offset = 0;
      bpm_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
      if (++page_index == page_number) {
        page_index = 0;
      }
      bpm_page = buffer_pool_manager_->FetchPage(page_ids_cache[page_index]);
      bpm_page->RLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(bpm_page->GetData());
    }
    if (page_index_start == page_index && slot_offset_start == slot_offset) {
      break;
    }
  }
  bpm_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
  table_latch_.RUnlock();
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert_Helper(Transaction *transaction, const KeyType &key, const ValueType &value) {
  const uint64_t hash_value = hash_fn_.GetHash(key);
  const size_t hash_position = hash_value % size_cache;
  const auto page_postion = GetPagePosition(hash_position);
  const size_t page_index_start = page_postion.first;
  const slot_offset_t slot_offset_start = page_postion.second;
  size_t page_index = page_postion.first;
  slot_offset_t slot_offset = page_postion.second;
  auto bpm_page = buffer_pool_manager_->FetchPage(page_ids_cache[page_index]);
  bpm_page->WLatch();
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(bpm_page->GetData());
  while (!block_page->Insert(slot_offset, key, value)) {
    if (block_page->IsReadable(slot_offset)
        && comparator_(key, block_page->KeyAt(slot_offset)) == 0
        && value == block_page->ValueAt(slot_offset)) {
      bpm_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
      return false;
    }
    // SAME SAME SAME
    if (++slot_offset == ((page_index == page_number - 1) ? BLOCK_ARRAY_SIZE_LAST_PAGE : BLOCK_ARRAY_SIZE_PRO_PAGE)) {
      slot_offset = 0;
      bpm_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
      if (++page_index == page_number) {
        page_index = 0;
      }
      bpm_page = buffer_pool_manager_->FetchPage(page_ids_cache[page_index]);
      bpm_page->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(bpm_page->GetData());
    }
    if (page_index_start == page_index && slot_offset_start == slot_offset) {
      bpm_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
      throw hash_table_full_error{};
    }
  }
  bpm_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], true);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
   bool insert_helper_res = false;
   try {
     insert_helper_res = Insert_Helper(transaction, key, value);
   } catch (hash_table_full_error) {
     table_latch_.RUnlock();
     throw hash_table_full_error{};
   }
   table_latch_.RUnlock();
   return insert_helper_res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  const uint64_t hash_value = hash_fn_.GetHash(key);
  table_latch_.RLock();
  const size_t hash_position = hash_value % size_cache;
  const auto page_postion = GetPagePosition(hash_position);
  const size_t page_index_start = page_postion.first;
  const slot_offset_t slot_offset_start = page_postion.second;
  size_t page_index = page_postion.first;
  slot_offset_t slot_offset = page_postion.second;
  auto bpm_page = buffer_pool_manager_->FetchPage(page_ids_cache[page_index]);
  bpm_page->WLatch();
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(bpm_page->GetData());
  while (block_page->IsOccupied(slot_offset)) {
    if (block_page->IsReadable(slot_offset)
        && comparator_(key, block_page->KeyAt(slot_offset)) == 0
        && value == block_page->ValueAt(slot_offset)) {
      block_page->Remove(slot_offset);
      bpm_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
      table_latch_.RUnlock();
      return true;
    }
    // SAME SAME SAME
    if (++slot_offset == ((page_index == page_number - 1) ? BLOCK_ARRAY_SIZE_LAST_PAGE : BLOCK_ARRAY_SIZE_PRO_PAGE)) {
      slot_offset = 0;
      bpm_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
      if (++page_index == page_number) {
        page_index = 0;
      }
      bpm_page = buffer_pool_manager_->FetchPage(page_ids_cache[page_index]);
      bpm_page->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(bpm_page->GetData());
    }
    if (page_index_start == page_index && slot_offset_start == slot_offset) {
      break;
    }
  }
  bpm_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  // TODO(jigao): initial size来自外界？？？
  table_latch_.WLock();

  // 1. In-memory cache all key-value pair
  std::vector<std::pair<KeyType, ValueType>> pair_cache;
//  pair_cache.reserve()
  assert(page_number == page_ids_cache.size());
  for (size_t page_index = 0; page_index < page_number; page_index++) {
    auto bpm_page = buffer_pool_manager_->FetchPage(page_ids_cache[page_index]);
    bpm_page->RLatch();
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(bpm_page->GetData());
    for (slot_offset_t slot_offset = 0;
         slot_offset < ((page_index == page_number - 1) ? BLOCK_ARRAY_SIZE_LAST_PAGE : BLOCK_ARRAY_SIZE_PRO_PAGE);
         slot_offset++) {
      if (block_page->IsReadable(slot_offset)) {
        pair_cache.emplace_back(block_page->KeyAt(slot_offset), block_page->ValueAt(slot_offset));
        block_page->Remove(slot_offset);
      }
    }
    bpm_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_ids_cache[page_index], false);
  }

  // 2. Resize
  const size_t old_page_number = page_number;
  const size_t num_buckets = initial_size * 2;
  assert((num_buckets - 1) / BLOCK_ARRAY_SIZE_PRO_PAGE + 1 > page_number);
  page_number = (num_buckets - 1) / BLOCK_ARRAY_SIZE_PRO_PAGE + 1;
  BLOCK_ARRAY_SIZE_LAST_PAGE = num_buckets - BLOCK_ARRAY_SIZE_PRO_PAGE * (page_number - 1);
  size_cache = num_buckets;

  // allocate block pages for the bucket number
  page_id_t page_id;
  for (size_t i = 0; i < page_number - old_page_number; i++) {
    buffer_pool_manager_->NewPage(&page_id);
    page_ids_cache.emplace_back(page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);
  }


  // 3. Update head page
  // 3.1. add block page id to the header page
  auto bpm_head_page = buffer_pool_manager_->FetchPage(header_page_id_);
  bpm_head_page->WLatch();
  auto ht_header_page = reinterpret_cast<HashTableHeaderPage *>(bpm_head_page->GetData());
  for (size_t i = old_page_number; i < page_number; i++) {
    ht_header_page->AddBlockPageId(page_ids_cache[i]);
  }
  // 3.2. update size
  ht_header_page->SetSize(num_buckets);
  bpm_head_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);

  // 4. Rehash
  for (const auto& pair : pair_cache) {
    bool insert_helper_res = false;
    try {
      insert_helper_res = Insert_Helper(nullptr, pair.first, pair.second);
    } catch (hash_table_full_error) {
      table_latch_.WUnlock();
      assert(false);  // SHOULD NOT HAPPEN!!
//      throw hash_table_full_error{};
    }
    assert(insert_helper_res);
  }
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  auto bpm_head_page = buffer_pool_manager_->FetchPage(header_page_id_);
  bpm_head_page->RLatch();
  auto ht_header_page = reinterpret_cast<HashTableHeaderPage *>(bpm_head_page->GetData());
  const auto size = ht_header_page->GetSize();
  bpm_head_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  assert(size == size_cache);
  table_latch_.RUnlock();
  return size;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
