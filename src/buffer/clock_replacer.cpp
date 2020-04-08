//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <utility>
#include <iterator>
#include <algorithm>
#include <functional>

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  clock_.reserve(num_pages);
  for (size_t i = 0; i < num_pages; i++) {
    clock_.emplace_back(NOT_EXISTS, NO_REF);
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 0. If the clock is empty, nothing to evict
  if (size_ == 0) {
    return false;
  }
  // 1. Get a victim
  while (true) {
    assert(clock_hand_ < clock_.size());
    auto& pair = clock_[clock_hand_];
    if (pair.first) {
      if (pair.second) {
        pair.second = NO_REF;
      } else {
        *frame_id = static_cast<frame_id_t >(clock_hand_);
        pair.first = NOT_EXISTS;
        size_--;
        return true;
      }
    }
    if (++clock_hand_ == clock_.size()) {
      clock_hand_ = 0;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(static_cast<size_t >(frame_id) <= clock_.size());
  auto& pair = clock_[frame_id];
  if (pair.first) {
    pair.first = NOT_EXISTS;
    size_--;
  }
  pair.second = NO_REF;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(static_cast<size_t >(frame_id) <= clock_.size());
  auto& pair = clock_[frame_id];
  if (!pair.first) {
    pair.first = EXISTS;
    size_++;
  }
  pair.second = REF;
}

size_t ClockReplacer::Size() {
  std::shared_lock<std::shared_mutex> shared_lock(latch_);
  return size_;
}
}  // namespace bustub
