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
#include <cassert>

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : num_pages_(num_pages) {
  ht_.reserve(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(ht_.size() == clock_.size());
  assert((clock_.size() == 0) ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
  // 0. If the clock is empty, nothing to evict
  if (clock_.size() == 0) return false;
  return Kill(frame_id);
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(ht_.size() == clock_.size());
  assert((clock_.size() == 0) ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
  // 1. remove from hash table, so frame_id is not in ClockReplacer
  // (Test says: no need to check, if it already in ClockReplacer.)
  // 2. remove from clock, if the frame id is in ClockReplacer
  // Pay attention to the case removing clock hand
  if (ht_.size() != 0) {
    auto clock_hand_it = std::next(clock_.begin(), clock_hand_);
    if (clock_hand_it->first == frame_id) {
      clock_.erase(clock_hand_it);
      if (clock_hand_ == clock_.size()) clock_hand_ = 0;
      ht_.erase(frame_id);
    } else {
      auto got = ht_.find(frame_id);
      if (got == ht_.end()) {
        return;
      } else {
        clock_.erase(got->second);
        ht_.erase(got);
      }
    }
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(ht_.size() == clock_.size());
  assert(ht_.size() <= num_pages_);
  assert((clock_.size() == 0) ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
  // 1. check if frame id already there
  auto got = ht_.find(frame_id);
  if (got == ht_.end()) {
    // 1.1.0. If Full
    if (ht_.size() == num_pages_) {
      frame_id_t mock_frame_id;
      Kill(&mock_frame_id);
    }
    // 1.1.1. not exists:= add element at the END of clock
    clock_.emplace_back(frame_id, true);
    // 1.1.2. insert into hash table
    ht_.emplace(frame_id, --clock_.end());
  } else {
    // 1.2. exists := INPLACE update the flag to true
    // (Test says: must be inplace update. Delete followed by Add is wrong!)
    got->second->second = true;
  }
}

size_t ClockReplacer::Size() {
  std::shared_lock<std::shared_mutex> shared_lock(latch_);
  // count set bits, indicating the number of pages in ClockReplacer
  assert(ht_.size() == clock_.size());
  assert((clock_.size() == 0) ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
  return ht_.size();
}

bool ClockReplacer::Kill(frame_id_t *frame_id) {
  // 1. while loop until find the victim
  // In Clock algorithm with non-empty clock, ALWAYS find a page to evict
  auto clock_hand_it = std::next(clock_.begin(), clock_hand_);
  while (true) {
    // 2. check ref bit
    if (!clock_hand_it->second) {
      // 2.1 ref bit := false, the victim found
      *frame_id = clock_hand_it->first;
      // 2.1.1 remove from hash table
      ht_.erase(*frame_id);
      // 2.1.2 update the clock hand and remove victim from clock
      clock_hand_ = std::distance(clock_.begin(), clock_hand_it);
      clock_.erase(clock_hand_it);
      if (clock_hand_ == clock_.size()) clock_hand_ = 0;
      return true;
    } else {
      // 2.1. ref bit := true
      clock_hand_it->second = false;
      // 2.2. next iteration acting as a CLOCK
      ++clock_hand_it;
      if (clock_hand_it == clock_.end()) clock_hand_it = clock_.begin();
    }
  }
}

}  // namespace bustub
