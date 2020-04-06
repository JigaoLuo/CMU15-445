//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer_test.cpp
//
// Identification: test/buffer/clock_replacer_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/clock_replacer.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ClockReplacerTest, SampleTest) {
  ClockReplacer clock_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(1);
  EXPECT_EQ(6, clock_replacer.Size());

  // Scenario: get three victims from the clock.
  int value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  clock_replacer.Pin(3);
  clock_replacer.Pin(4);
  EXPECT_EQ(2, clock_replacer.Size());

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  clock_replacer.Unpin(4);

  // Scenario: continue looking for victims. We expect these victims.
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}

TEST(ClockReplacerTest, UnpinTest) {
  constexpr size_t num_pages = 1000;
  constexpr frame_id_t insert_times = 1200;
  static_assert(num_pages <= insert_times);
  ClockReplacer clock_replacer(num_pages);

  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Unpin(i);
  }
  EXPECT_EQ(num_pages, clock_replacer.Size());
}

TEST(ClockReplacerTest, PinTest) {
  constexpr size_t num_pages = 1000;
  constexpr frame_id_t insert_times = 1200;
  static_assert(num_pages <= insert_times);
  ClockReplacer clock_replacer(num_pages);

  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Unpin(i);
  }
  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Pin(i);
  }
  EXPECT_EQ(0, clock_replacer.Size());
}

TEST(ClockReplacerTest, PinUnpinTest) {
  constexpr size_t num_pages = 1000;
  constexpr frame_id_t insert_times = 1200;
  static_assert(num_pages <= insert_times);
  ClockReplacer clock_replacer(num_pages);

  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Unpin(i);
  }
  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Pin(i);
    clock_replacer.Unpin(i);
    clock_replacer.Pin(i);
  }
  EXPECT_EQ(0, clock_replacer.Size());
  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Unpin(i);
  }
  EXPECT_EQ(num_pages, clock_replacer.Size());
}

TEST(ClockReplacerTest, VictimTest) {
  constexpr size_t num_pages = 1000;
  constexpr frame_id_t insert_times = 1200;
  static_assert(num_pages <= insert_times);
  ClockReplacer clock_replacer(num_pages);

  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Unpin(i);
  }
  frame_id_t frame_id;
  for (size_t i = 0; i < num_pages; i++) {
    EXPECT_TRUE(clock_replacer.Victim(&frame_id));
  }
  EXPECT_FALSE(clock_replacer.Victim(&frame_id));
}

}  // namespace bustub
