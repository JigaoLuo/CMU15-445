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

TEST(ClockReplacerTest, SampleTest2) {
  constexpr size_t num_pages = 1000;
  constexpr frame_id_t insert_times = 800;
  static_assert(num_pages > insert_times);
  ClockReplacer clock_replacer(num_pages);

  // Scenario: unpin `insert_times` elements, i.e. add them to the replacer.
  for (frame_id_t i = 1; i <= insert_times; i++) {
    clock_replacer.Unpin(i);
  }
  clock_replacer.Unpin(1);
  EXPECT_EQ(static_cast<size_t >(insert_times), clock_replacer.Size());

  // Scenario: get `insert_times` victims from the clock.
  int value;
  for (frame_id_t i = 1; i <= insert_times; i++) {
    EXPECT_TRUE(clock_replacer.Victim(&value));
    EXPECT_EQ(i, value);
  }
  EXPECT_EQ(0, clock_replacer.Size());


  // Scenario: unpin `insert_times` elements, i.e. add them to the replacer.
  for (frame_id_t i = 1; i <= insert_times; i++) {
    clock_replacer.Unpin(i);
  }

  // Scenario: get 25% of `insert_times` victims from the clock.
  for (frame_id_t i = 1; i <= insert_times / 4; i++) {
    EXPECT_TRUE(clock_replacer.Victim(&value));
    EXPECT_EQ(i, value);
  }
  EXPECT_EQ(static_cast<size_t >(insert_times) * 3 / 4, clock_replacer.Size());

  // Scenario: pin elements in the replacer.
  // Note that some have already been victimized, so pinning them should have no effect.
  for (frame_id_t i = 1; i <= insert_times / 2; i++) {
    clock_replacer.Pin(i);
  }
  EXPECT_EQ(static_cast<size_t >(insert_times) / 2, clock_replacer.Size());

  // Scenario: unpin the remaining first one. We expect that the reference bit of 4 will be set to 1.
  clock_replacer.Unpin(insert_times / 2 + 1);

  // Scenario: continue looking for victims. We expect these victims.
  for (frame_id_t i = insert_times / 2 + 1; i < insert_times; i++) {
    EXPECT_TRUE(clock_replacer.Victim(&value));
    EXPECT_EQ(i + 1, value);
  }
  EXPECT_TRUE(clock_replacer.Victim(&value));
  EXPECT_EQ(insert_times / 2 + 1, value);
  EXPECT_EQ(0, clock_replacer.Size());
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
  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Pin(i);
  }
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
