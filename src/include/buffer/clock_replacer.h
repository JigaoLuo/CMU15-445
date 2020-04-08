//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <shared_mutex>  // NOLINT
#include <vector>
#include <unordered_map>
#include <utility>
#include <cassert>

#include "buffer/replacer.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  /**
   * Starting from the current position of clock hand, find the first frame that is both in the ClockReplacer and
   * with its ref flag set to false. If a frame is in the ClockReplacer, but its ref flag is set to true,
   * change it to false instead. This should be the only method that updates the clock hand.
   * @param frame_id  the first frame that is both in the ClockReplacer and with its ref flag set to false.
   * @return true for frame victimzed, otherwise false
   */
  bool Victim(frame_id_t *frame_id) override;

  /**
   * This method should be called after a page is pinned to a frame in the Buffer Pool Manager.
   * It should remove the frame containing the pinned page from the Clock Replacer.
   * @param frame_id  the page is pinned to a frame in the Buffer Pool Manager.
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * This method should be called when the pin_count of a page becomes 0.
   * This method should add the frame containing the unpinned page to the ClockReplacer.
   * @param frame_id  the page having pin_count becoming 0
   */
  void Unpin(frame_id_t frame_id) override;

  /** @return the number of frames that are currently in the ClockReplacer. */
  size_t Size() override;

  /** @return get the position of clock hand */
  size_t GetClockHand() {
    std::shared_lock<std::shared_mutex> shared_lock(latch_);
    return clock_hand_;
  }

 private:
  using exist_bit_t = bool;
  static constexpr exist_bit_t NOT_EXISTS = false;
  static constexpr exist_bit_t EXISTS = true;
  using reference_bit_t = bool;
  static constexpr reference_bit_t NO_REF = false;
  static constexpr reference_bit_t REF = true;

  /** Number of current loaded pages */
  size_t size_ = 0;
  /** List acts as a clock. Each element is a pair of frame id and the ref flag */
  std::vector<std::pair<exist_bit_t, reference_bit_t>> clock_;
  /** the position in list := clock hand position */
  size_t clock_hand_ = 0;
  /** Latch */
  mutable std::shared_mutex latch_;
};

}  // namespace bustub
