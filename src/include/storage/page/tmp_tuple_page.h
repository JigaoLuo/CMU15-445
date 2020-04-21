#pragma once

#include <string>  // TODO(jigao): delete this

#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpacePointer (4) | (free space) | TupleSize2(4) | TupleData2 | TupleSize1(4) | TupleData1 |
 *                                                              ^
 *                                                              free space pointer
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
class TmpTuplePage : public Page {
 public:
  void Init(page_id_t page_id, uint32_t page_size) {
    // Set the page ID. => the [0, 4) byte
    memcpy(GetData(), &page_id, sizeof(page_id));
    // Not to set LSN   => the [4, 8) byte
    // Set free space   => the [8, 12) bytes
    SetFreeSpacePointer(page_size);
  }

  inline page_id_t GetTmpTuplePageId() { return *reinterpret_cast<page_id_t *>(GetData()); }

  bool Insert(const Tuple &tuple, TmpTuple *out) {
    BUSTUB_ASSERT(tuple.size_ > 0, "Cannot have empty tuples.");
    // If there is not enough space, then return false.
    if (GetFreeSpaceRemaining() < tuple.size_ + TUPLE_SIZE) {
      return false;
    }
    SetFreeSpacePointer(GetFreeSpacePointer() - tuple.size_);
    memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.size_);
    SetFreeSpacePointer(GetFreeSpacePointer() - TUPLE_SIZE);
    memcpy(GetData() + GetFreeSpacePointer(), &tuple.size_, sizeof(uint32_t));
    out->SetPageId(GetTmpTuplePageId());
    out->SetOffset(GetFreeSpacePointer());
    return true;
  }

  /** Gets free space remaining size. */
  inline uint32_t GetFreeSpaceRemaining() {
    return GetFreeSpacePointer() - SIZE_TABLE_PAGE_HEADER;
  }

  /** Gets free space pointer. */
  inline uint32_t GetFreeSpacePointer() {
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE);
  }

 private:
  static_assert(sizeof(page_id_t) == 4);

  static constexpr size_t SIZE_TABLE_PAGE_HEADER = 12;    // for: | PageId (4) | LSN (4) | FreeSpacePointer (4) |
  static constexpr size_t OFFSET_FREE_SPACE = 8;
  static constexpr size_t TUPLE_SIZE = 4;

  /** Sets free space size. */
  inline void SetFreeSpacePointer(uint32_t free_space) {
    memcpy(GetData() + OFFSET_FREE_SPACE, &free_space, sizeof(uint32_t));
  }
};

}  // namespace bustub
