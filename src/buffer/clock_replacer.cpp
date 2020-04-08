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

ClockReplacer::ClockReplacer(size_t num_pages) : num_pages_(num_pages) {
  std::cout << "num_pages: " << num_pages << " || ";
  ht_.reserve(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(ht_.size() == clock_.size());
  assert(clock_.empty() ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
  std::cout << "Victim || ";
  // 0. If the clock is empty, nothing to evict
  if (clock_.empty()) {
    return false;
  }
  auto res =  Kill(frame_id);
  if (clock_hand_ == clock_.size()) {
    clock_hand_ = 0;
  }
  return res;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(ht_.size() == clock_.size());
  assert(clock_.empty() ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
  // 1. remove from hash table, so frame_id is not in ClockReplacer
  // (Test says: no need to check, if it already in ClockReplacer.)
  // 2. remove from clock, if the frame id is in ClockReplacer
  // Pay attention to the case removing clock hand
  std::cout << "Pin: " << frame_id << " || ";
  if (!ht_.empty()) {
    auto clock_hand_it = std::next(clock_.begin(), clock_hand_);
    if (clock_hand_it->first == frame_id) {
      clock_.erase(clock_hand_it);
      if (clock_hand_ == clock_.size()) {
        clock_hand_ = 0;
      }
      ht_.erase(frame_id);
    } else {
      auto got = ht_.find(frame_id);
      if (got == ht_.end()) {
        return;
      }
      clock_.erase(got->second);
      ht_.erase(got);
    }
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  assert(ht_.size() == clock_.size());
  assert(ht_.size() <= num_pages_);
  assert(clock_.empty() ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
  std::cout << "Unpin: " << frame_id << " || ";
  // 1. check if frame id already there
  auto got = ht_.find(frame_id);
  if (got == ht_.end()) {
    // 1.1. If Full, in-inplace insert
    if (ht_.size() == num_pages_) {
      // 1.1.1. kill a frame to have space
      frame_id_t mock_frame_id;
      Kill(&mock_frame_id);
      // 1.1.2. inplace insert - a.k.a the killed position
      auto it = std::next(clock_.begin(), clock_hand_);
      clock_.emplace(it, frame_id, true);
      ht_.emplace(frame_id, it);
      // 1.1.3 move to next position
      if (++clock_hand_ == clock_.size()) {
        clock_hand_ = 0;
      }
    } else {
      // 1.2. not full, add element at the END of clock
      clock_.emplace_back(frame_id, true);
      ht_.emplace(frame_id, --clock_.end());
    }
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
  assert(clock_.empty() ? (clock_hand_ == 0) : (clock_hand_ < clock_.size()));
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
      return true;
    }
    // 2.1. ref bit := true
    clock_hand_it->second = false;
    // 2.2. next iteration acting as a CLOCK
    ++clock_hand_it;
    if (clock_hand_it == clock_.end()) {
      clock_hand_it = clock_.begin();
    }
  }
}

}  // namespace bustub
