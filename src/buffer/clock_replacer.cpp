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
  // num_pages ONLY known in RUNTIME, so resize the bitset to the number of pages(frames)
  page_placeholders.resize(num_pages);

  // reset all bits to 0
  page_placeholders.reset();
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  // 1. locate the clock hand as iteration start
  auto it = std::next(clock.begin(), clock_hand);

  // 2. while loop until find the victim (ALWAYS find one)
  while (true) {
    auto& pair = *it;

    // 3. check ref bit
    if (!pair.second) {
      // 3.1 ref bit := false a.k.a THE VICTIM
      *frame_id = std::get<0>(pair);

      // 3.1.1 remove from bitset
      page_placeholders.reset(*frame_id);

      // 3.1.2 update the clock hand (I update it only when the victim is found)
      clock_hand = std::distance(clock.begin(), it);

      // 3.1.3 remove from clock
      clock.erase(it);

      return true;
    } else {
      // 3.1 ref bit := true
      pair.second = false;
    }

    // 4. next iteration acting as a CLOCK
    ++it;
    if (it == clock.end()) {
      it = clock.begin();
    }
  }
  // In Clock algorithm, ALWAYS find a page to evict
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  // 1. remove from bitset, so frame_id is not in ClockReplacer -- (Test says: no need to check, if it already in ClockReplacer.)
  page_placeholders.reset(frame_id);

  // 2. remove from clock, if the frame id is in ClockReplacer
  clock.remove_if( [frame_id](std::pair<frame_id_t, bool> n){ return n.first == frame_id; } );
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  // 1. check if frame id already there
  if (!page_placeholders.test(frame_id)) {
    // 1.1.1 not exists:= add element at the END of clock
    clock.emplace_back(frame_id, true);

    // 1.1.2 set the frame_id bit in bitset
    page_placeholders.set(frame_id);
  } else {
    // 1.2 exists := INPLACE update the flag to true -- (Test says: must be inplace update. Delete followed by Add is wrong!)
    auto pos = find(clock.begin(), clock.end(), std::make_pair(frame_id, false));
    if(pos != clock.end()) {
      pos->second = true;
    }
  }
}

size_t ClockReplacer::Size() {
  // count set bits, indicating the number of pages in ClockReplacer
  assert(page_placeholders.count() == clock.size());
  return page_placeholders.count();
}

}  // namespace bustub
