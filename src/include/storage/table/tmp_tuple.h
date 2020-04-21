#pragma once

#include "common/config.h"

namespace bustub {

// TODO(jigao): Document this class! What does it represent? hash key, hash value

class TmpTuple {
 public:
  TmpTuple(page_id_t page_id, size_t offset) : page_id_(page_id), offset_(offset) {}

  inline bool operator==(const TmpTuple &rhs) const { return page_id_ == rhs.page_id_ && offset_ == rhs.offset_; }

  page_id_t GetPageId() const { return page_id_; }
  size_t GetOffset() const { return offset_; }
  void SetPageId(page_id_t page_id) { page_id_ = page_id; }
  void SetOffset(size_t offset) { offset_ = offset; }

 private:
  page_id_t page_id_;
  size_t offset_;
};

}  // namespace bustub
