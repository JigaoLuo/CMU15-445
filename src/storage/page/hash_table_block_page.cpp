//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"
#include "common/util/hash_util.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

static constexpr uint8_t N_TH_BIT_MASK[8] = { 0b1000'0000, 0b0100'0000, 0b0010'0000, 0b0001'0000,
                                              0b0000'1000, 0b0000'0100, 0b0000'0010, 0b0000'0001 };

inline static bool GET_N_TH_BIT(const std::atomic_char* char_arr, slot_offset_t bucket_ind) {
  const auto arr_index = bucket_ind >> 3;                  // bucket_ind / 8
  const auto bit_offset = bucket_ind - (arr_index << 3);   // bucket_ind % 8
  return (char_arr[arr_index] & N_TH_BIT_MASK[bit_offset]) != 0;
}

inline static void SET_N_TH_BIT(std::atomic_char *char_arr, slot_offset_t bucket_ind) {
  const auto arr_index = bucket_ind >> 3;                  // bucket_ind / 8
  const auto bit_offset = bucket_ind - (arr_index << 3);   // bucket_ind % 8
  char_arr[arr_index] |= N_TH_BIT_MASK[bit_offset];
}

inline static void UNSET_N_TH_BIT(std::atomic_char *char_arr, slot_offset_t bucket_ind) {
  const auto arr_index = bucket_ind >> 3;                  // bucket_ind / 8
  const auto bit_offset = bucket_ind - (arr_index << 3);   // bucket_ind % 8
  char_arr[arr_index] &= ~N_TH_BIT_MASK[bit_offset];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  assert(GET_N_TH_BIT(readable_, bucket_ind));
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  assert(GET_N_TH_BIT(readable_, bucket_ind));
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  if (GET_N_TH_BIT(readable_, bucket_ind)) {
    return false;  // NOLINT
  }
  SET_N_TH_BIT(occupied_, bucket_ind);
  SET_N_TH_BIT(readable_, bucket_ind);
  array_[bucket_ind] = {key, value};
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  assert(GET_N_TH_BIT(readable_, bucket_ind));
  UNSET_N_TH_BIT(readable_, bucket_ind);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  return GET_N_TH_BIT(occupied_, bucket_ind);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  return GET_N_TH_BIT(readable_, bucket_ind);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<hash_t, TmpTuple, HashComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
