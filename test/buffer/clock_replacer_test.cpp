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

// NOLINTNEXTLINE
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

// NOLINTNEXTLINE
//TEST(ClockReplacerTest, SampleTest2) {
//  constexpr size_t num_pages = 1000;
//  constexpr frame_id_t insert_times = 800;
//  static_assert(num_pages > insert_times);
//  ClockReplacer clock_replacer(num_pages);
//
//  // Scenario: unpin `insert_times` elements, i.e. add them to the replacer.
//  for (frame_id_t i = 1; i <= insert_times; i++) {
//    clock_replacer.Unpin(i);
//  }
//  clock_replacer.Unpin(1);
//  EXPECT_EQ(static_cast<size_t >(insert_times), clock_replacer.Size());
//
//  // Scenario: get `insert_times` victims from the clock.
//  int value;
//  for (frame_id_t i = 1; i <= insert_times; i++) {
//    EXPECT_TRUE(clock_replacer.Victim(&value));
//    EXPECT_EQ(i, value);
//  }
//  EXPECT_EQ(0, clock_replacer.Size());
//
//
//  // Scenario: unpin `insert_times` elements, i.e. add them to the replacer.
//  for (frame_id_t i = 1; i <= insert_times; i++) {
//    clock_replacer.Unpin(i);
//  }
//
//  // Scenario: get 25% of `insert_times` victims from the clock.
//  for (frame_id_t i = 1; i <= insert_times / 4; i++) {
//    EXPECT_TRUE(clock_replacer.Victim(&value));
//    EXPECT_EQ(i, value);
//  }
//  EXPECT_EQ(static_cast<size_t >(insert_times) * 3 / 4, clock_replacer.Size());
//
//  // Scenario: pin elements in the replacer.
//  // Note that some have already been victimized, so pinning them should have no effect.
//  for (frame_id_t i = 1; i <= insert_times / 2; i++) {
//    clock_replacer.Pin(i);
//  }
//  EXPECT_EQ(static_cast<size_t >(insert_times) / 2, clock_replacer.Size());
//
//  // Scenario: unpin the remaining first one. We expect that the reference bit of 4 will be set to 1.
//  clock_replacer.Unpin(insert_times / 2 + 1);
//
//  // Scenario: continue looking for victims. We expect these victims.
//  for (frame_id_t i = insert_times / 2 + 1; i < insert_times; i++) {
//    EXPECT_TRUE(clock_replacer.Victim(&value));
//    EXPECT_EQ(i + 1, value);
//  }
//  EXPECT_TRUE(clock_replacer.Victim(&value));
//  EXPECT_EQ(insert_times / 2 + 1, value);
//  EXPECT_EQ(0, clock_replacer.Size());
//}
//
//// Added by Jigao
//// See slide: https://www.cs.columbia.edu/~junfeng/11sp-w4118/lectures/l19-vm.pdf
//// NOLINTNEXTLINE
//TEST(ClockReplacerTest, SlideTest) {
//  constexpr size_t num_pages = 4;
//  ClockReplacer clock_replacer(num_pages);
//  for (size_t i = 1; i <= num_pages; i++) {
//    clock_replacer.Unpin(static_cast<frame_id_t >(i));
//    EXPECT_EQ(0, clock_replacer.GetClockHand());
//  }
//  EXPECT_EQ((std::vector<frame_id_t>{1, 2, 3, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(5));
//  EXPECT_EQ(1, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 2, 3, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 0, 0, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(1));
//  EXPECT_EQ(2, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 1, 3, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 0, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(2));
//  EXPECT_EQ(3, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 1, 2, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(3));
//  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 1, 2, 3}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(4));
//  EXPECT_EQ(1, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{4, 1, 2, 3}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 0, 0, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(5));
//  EXPECT_EQ(2, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{4, 5, 2, 3}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 0, 0}), clock_replacer.GetRefList());
//}
//
//// Added by Jigao
//// See stackoverflow: https://cs.stackexchange.com/a/24017
//// NOLINTNEXTLINE
//TEST(ClockReplacerTest, SOFTest) {
//  // TODO: GetClockHand
//  constexpr size_t num_pages = 5;
//  ClockReplacer clock_replacer(num_pages);
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(3));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{3}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(2));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{3, 2}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(3));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{3, 2}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(0));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{3, 2, 0}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(8));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{3, 2, 0, 8}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(4));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{3, 2, 0, 8, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(2));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{3, 2, 0, 8, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 1, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(5));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 2, 0, 8, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 0, 0, 0, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(0));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 2, 0, 8, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 0, 1, 0, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(9));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 9, 0, 8, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 0, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(8));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 9, 0, 8, 4}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 1, 1, 0}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(3));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 9, 0, 8, 3}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{1, 1, 0, 0, 1}), clock_replacer.GetRefList());
//
//  clock_replacer.Unpin(static_cast<frame_id_t >(2));
////  EXPECT_EQ(0, clock_replacer.GetClockHand());
//  EXPECT_EQ((std::vector<frame_id_t>{5, 9, 2, 8, 3}), clock_replacer.GetFrameList());
//  EXPECT_EQ((std::vector<int>{0, 0, 1, 0, 1}), clock_replacer.GetRefList());
//}

// Added by Jigao
// printed from gradescope
// NOLINTNEXTLINE
TEST(ClockReplacerTest, Victim) {
  constexpr size_t num_pages = 1010;
  ClockReplacer clock_replacer(num_pages);

  int value;
  EXPECT_FALSE(clock_replacer.Victim(&value));

  clock_replacer.Unpin(11);
  clock_replacer.Victim(&value);
  EXPECT_EQ(11, value);

  clock_replacer.Unpin(1);
  clock_replacer.Unpin(1);
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);

  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(10);
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(10, value);
  EXPECT_FALSE(clock_replacer.Victim(&value));

  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(7);
  clock_replacer.Unpin(8);
  clock_replacer.Unpin(6);
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  clock_replacer.Unpin(7);
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(8, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(7, value);
  EXPECT_FALSE(clock_replacer.Victim(&value));

  clock_replacer.Unpin(10);
  clock_replacer.Unpin(10);
  clock_replacer.Victim(&value);
  EXPECT_EQ(10, value);
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_FALSE(clock_replacer.Victim(&value));

  constexpr frame_id_t insert_times = 1000;
  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Unpin(i);
  }

  for (frame_id_t i = 0; i < insert_times; i++) {
    clock_replacer.Victim(&value);
    EXPECT_EQ((10 + i) % 1000 , value);
  }

// Unpin: 0 || Unpin: 1 || Unpin: 2 || Unpin: 3 || Unpin: 4 || Unpin: 5 || Unpin: 6 || Unpin: 7 || Unpin: 8 || Unpin: 9 ||
// Unpin: 10 || Unpin: 11 || Unpin: 12 || Unpin: 13 || Unpin: 14 || Unpin: 15 || Unpin: 16 || Unpin: 17 || Unpin: 18 || Unpin: 19 ||
// Unpin: 20 || Unpin: 21 || Unpin: 22 || Unpin: 23 || Unpin: 24 || Unpin: 25 || Unpin: 26 || Unpin: 27 || Unpin: 28 || Unpin: 29 ||
// Unpin: 30 || Unpin: 31 || Unpin: 32 || Unpin: 33 || Unpin: 34 || Unpin: 35 || Unpin: 36 || Unpin: 37 || Unpin: 38 || Unpin: 39 ||
// Unpin: 40 || Unpin: 41 || Unpin: 42 || Unpin: 43 || Unpin: 44 || Unpin: 45 || Unpin: 46 || Unpin: 47 || Unpin: 48 || Unpin: 49 ||
// Unpin: 50 || Unpin: 51 || Unpin: 52 || Unpin: 53 || Unpin: 54 || Unpin: 55 || Unpin: 56 || Unpin: 57 || Unpin: 58 || Unpin: 59 ||
// Unpin: 60 || Unpin: 61 || Unpin: 62 || Unpin: 63 || Unpin: 64 || Unpin: 65 || Unpin: 66 || Unpin: 67 || Unpin: 68 || Unpin: 69 ||
// Unpin: 70 || Unpin: 71 || Unpin: 72 || Unpin: 73 || Unpin: 74 || Unpin: 75 || Unpin: 76 || Unpin: 77 || Unpin: 78 || Unpin: 79 ||
// Unpin: 80 || Unpin: 81 || Unpin: 82 || Unpin: 83 || Unpin: 84 || Unpin: 85 || Unpin: 86 || Unpin: 87 || Unpin: 88 || Unpin: 89 ||
// Unpin: 90 || Unpin: 91 || Unpin: 92 || Unpin: 93 || Unpin: 94 || Unpin: 95 || Unpin: 96 || Unpin: 97 || Unpin: 98 || Unpin: 99 ||
//
// Unpin: 100 || Unpin: 101 || Unpin: 102 || Unpin: 103 || Unpin: 104 || Unpin: 105 || Unpin: 106 || Unpin: 107 || Unpin: 108 || Unpin: 109 ||
// Unpin: 110 || Unpin: 111 || Unpin: 112 || Unpin: 113 || Unpin: 114 || Unpin: 115 || Unpin: 116 || Unpin: 117 || Unpin: 118 || Unpin: 119 ||
// Unpin: 120 || Unpin: 121 || Unpin: 122 || Unpin: 123 || Unpin: 124 || Unpin: 125 || Unpin: 126 || Unpin: 127 || Unpin: 128 || Unpin: 129 ||
// Unpin: 130 || Unpin: 131 || Unpin: 132 || Unpin: 133 || Unpin: 134 || Unpin: 135 || Unpin: 136 || Unpin: 137 || Unpin: 138 || Unpin: 139 ||
// Unpin: 140 || Unpin: 141 || Unpin: 142 || Unpin: 143 || Unpin: 144 || Unpin: 145 || Unpin: 146 || Unpin: 147 || Unpin: 148 || Unpin: 149 ||
// Unpin: 150 || Unpin: 151 || Unpin: 152 || Unpin: 153 || Unpin: 154 || Unpin: 155 || Unpin: 156 || Unpin: 157 || Unpin: 158 || Unpin: 159 ||
// Unpin: 160 || Unpin: 161 || Unpin: 162 || Unpin: 163 || Unpin: 164 || Unpin: 165 || Unpin: 166 || Unpin: 167 || Unpin: 168 || Unpin: 169 ||
// Unpin: 170 || Unpin: 171 || Unpin: 172 || Unpin: 173 || Unpin: 174 || Unpin: 175 || Unpin: 176 || Unpin: 177 || Unpin: 178 || Unpin: 179 ||
// Unpin: 180 || Unpin: 181 || Unpin: 182 || Unpin: 183 || Unpin: 184 || Unpin: 185 || Unpin: 186 || Unpin: 187 || Unpin: 188 || Unpin: 189 ||
// Unpin: 190 || Unpin: 191 || Unpin: 192 || Unpin: 193 || Unpin: 194 || Unpin: 195 || Unpin: 196 || Unpin: 197 || Unpin: 198 || Unpin: 199 ||
// Unpin: 200 || Unpin: 201 || Unpin: 202 || Unpin: 203 || Unpin: 204 || Unpin: 205 || Unpin: 206 || Unpin: 207 || Unpin: 208 || Unpin: 209 ||

// Unpin: 210 || Unpin: 211 || Unpin: 212 || Unpin: 213 || Unpin: 214 || Unpin: 215 || Unpin: 216 || Unpin: 217 || Unpin: 218 || Unpin: 219 ||
// Unpin: 220 || Unpin: 221 || Unpin: 222 || Unpin: 223 || Unpin: 224 || Unpin: 225 || Unpin: 226 || Unpin: 227 || Unpin: 228 || Unpin: 229 ||
// Unpin: 230 || Unpin: 231 || Unpin: 232 || Unpin: 233 || Unpin: 234 || Unpin: 235 || Unpin: 236 || Unpin: 237 || Unpin: 238 || Unpin: 239 ||
// Unpin: 240 || Unpin: 241 || Unpin: 242 || Unpin: 243 || Unpin: 244 || Unpin: 245 || Unpin: 246 || Unpin: 247 || Unpin: 248 || Unpin: 249 ||
// Unpin: 250 || Unpin: 251 || Unpin: 252 || Unpin: 253 || Unpin: 254 || Unpin: 255 || Unpin: 256 || Unpin: 257 || Unpin: 258 || Unpin: 259 ||
// Unpin: 260 || Unpin: 261 || Unpin: 262 || Unpin: 263 || Unpin: 264 || Unpin: 265 || Unpin: 266 || Unpin: 267 || Unpin: 268 || Unpin: 269 ||
// Unpin: 270 || Unpin: 271 || Unpin: 272 || Unpin: 273 || Unpin: 274 || Unpin: 275 || Unpin: 276 || Unpin: 277 || Unpin: 278 || Unpin: 279 ||
// Unpin: 280 || Unpin: 281 || Unpin: 282 || Unpin: 283 || Unpin: 284 || Unpin: 285 || Unpin: 286 || Unpin: 287 || Unpin: 288 || Unpin: 289 ||
// Unpin: 290 || Unpin: 291 || Unpin: 292 || Unpin: 293 || Unpin: 294 || Unpin: 295 || Unpin: 296 || Unpin: 297 || Unpin: 298 || Unpin: 299 ||

// Unpin: 300 || Unpin: 301 || Unpin: 302 || Unpin: 303 || Unpin: 304 || Unpin: 305 || Unpin: 306 || Unpin: 307 || Unpin: 308 || Unpin: 309 ||
// Unpin: 310 || Unpin: 311 || Unpin: 312 || Unpin: 313 || Unpin: 314 || Unpin: 315 || Unpin: 316 || Unpin: 317 || Unpin: 318 || Unpin: 319 ||
// Unpin: 320 || Unpin: 321 || Unpin: 322 || Unpin: 323 || Unpin: 324 || Unpin: 325 || Unpin: 326 || Unpin: 327 || Unpin: 328 || Unpin: 329 ||
// Unpin: 330 || Unpin: 331 || Unpin: 332 || Unpin: 333 || Unpin: 334 || Unpin: 335 || Unpin: 336 || Unpin: 337 || Unpin: 338 || Unpin: 339 ||
// Unpin: 340 || Unpin: 341 || Unpin: 342 || Unpin: 343 || Unpin: 344 || Unpin: 345 || Unpin: 346 || Unpin: 347 || Unpin: 348 || Unpin: 349 ||
// Unpin: 350 || Unpin: 351 || Unpin: 352 || Unpin: 353 || Unpin: 354 || Unpin: 355 || Unpin: 356 || Unpin: 357 || Unpin: 358 || Unpin: 359 ||
// Unpin: 360 || Unpin: 361 || Unpin: 362 || Unpin: 363 || Unpin: 364 || Unpin: 365 || Unpin: 366 || Unpin: 367 || Unpin: 368 || Unpin: 369 ||
// Unpin: 370 || Unpin: 371 || Unpin: 372 || Unpin: 373 || Unpin: 374 || Unpin: 375 || Unpin: 376 || Unpin: 377 || Unpin: 378 || Unpin: 379 ||
// Unpin: 380 || Unpin: 381 || Unpin: 382 || Unpin: 383 || Unpin: 384 || Unpin: 385 || Unpin: 386 || Unpin: 387 || Unpin: 388 || Unpin: 389 ||
// Unpin: 390 || Unpin: 391 || Unpin: 392 || Unpin: 393 || Unpin: 394 || Unpin: 395 || Unpin: 396 || Unpin: 397 || Unpin: 398 || Unpin: 399 ||

// Unpin: 400 || Unpin: 401 || Unpin: 402 || Unpin: 403 || Unpin: 404 || Unpin: 405 || Unpin: 406 || Unpin: 407 || Unpin: 408 || Unpin: 409 ||
// Unpin: 410 || Unpin: 411 || Unpin: 412 || Unpin: 413 || Unpin: 414 || Unpin: 415 || Unpin: 416 || Unpin: 417 || Unpin: 418 || Unpin: 419 ||
// Unpin: 420 || Unpin: 421 || Unpin: 422 || Unpin: 423 || Unpin: 424 || Unpin: 425 || Unpin: 426 || Unpin: 427 || Unpin: 428 || Unpin: 429 ||
// Unpin: 430 || Unpin: 431 || Unpin: 432 || Unpin: 433 || Unpin: 434 || Unpin: 435 || Unpin: 436 || Unpin: 437 || Unpin: 438 || Unpin: 439 ||
// Unpin: 440 || Unpin: 441 || Unpin: 442 || Unpin: 443 || Unpin: 444 || Unpin: 445 || Unpin: 446 || Unpin: 447 || Unpin: 448 || Unpin: 449 ||
// Unpin: 450 || Unpin: 451 || Unpin: 452 || Unpin: 453 || Unpin: 454 || Unpin: 455 || Unpin: 456 || Unpin: 457 || Unpin: 458 || Unpin: 459 ||
// Unpin: 460 || Unpin: 461 || Unpin: 462 || Unpin: 463 || Unpin: 464 || Unpin: 465 || Unpin: 466 || Unpin: 467 || Unpin: 468 || Unpin: 469 ||
// Unpin: 470 || Unpin: 471 || Unpin: 472 || Unpin: 473 || Unpin: 474 || Unpin: 475 || Unpin: 476 || Unpin: 477 || Unpin: 478 || Unpin: 479 ||
// Unpin: 480 || Unpin: 481 || Unpin: 482 || Unpin: 483 || Unpin: 484 || Unpin: 485 || Unpin: 486 || Unpin: 487 || Unpin: 488 || Unpin: 489 ||
// Unpin: 490 || Unpin: 491 || Unpin: 492 || Unpin: 493 || Unpin: 494 || Unpin: 495 || Unpin: 496 || Unpin: 497 || Unpin: 498 || Unpin: 499 ||
//
// Unpin: 500 || Unpin: 501 || Unpin: 502 || Unpin: 503 || Unpin: 504 || Unpin: 505 || Unpin: 506 || Unpin: 507 || Unpin: 508 || Unpin: 509 ||
// Unpin: 510 || Unpin: 511 || Unpin: 512 || Unpin: 513 || Unpin: 514 || Unpin: 515 || Unpin: 516 || Unpin: 517 || Unpin: 518 || Unpin: 519 ||
// Unpin: 520 || Unpin: 521 || Unpin: 522 || Unpin: 523 || Unpin: 524 || Unpin: 525 || Unpin: 526 || Unpin: 527 || Unpin: 528 || Unpin: 529 ||
// Unpin: 530 || Unpin: 531 || Unpin: 532 || Unpin: 533 || Unpin: 534 || Unpin: 535 || Unpin: 536 || Unpin: 537 || Unpin: 538 || Unpin: 539 ||
// Unpin: 540 || Unpin: 541 || Unpin: 542 || Unpin: 543 || Unpin: 544 || Unpin: 545 || Unpin: 546 || Unpin: 547 || Unpin: 548 || Unpin: 549 ||
// Unpin: 550 || Unpin: 551 || Unpin: 552 || Unpin: 553 || Unpin: 554 || Unpin: 555 || Unpin: 556 || Unpin: 557 || Unpin: 558 || Unpin: 559 ||
// Unpin: 560 || Unpin: 561 || Unpin: 562 || Unpin: 563 || Unpin: 564 || Unpin: 565 || Unpin: 566 || Unpin: 567 || Unpin: 568 || Unpin: 569 ||
// Unpin: 570 || Unpin: 571 || Unpin: 572 || Unpin: 573 || Unpin: 574 || Unpin: 575 || Unpin: 576 || Unpin: 577 || Unpin: 578 || Unpin: 579 || U
// npin: 580 || Unpin: 581 || Unpin: 582 || Unpin: 583 || Unpin: 584 || Unpin: 585 || Unpin: 586 || Unpin: 587 || Unpin: 588 || Unpin: 589 ||
// Unpin: 590 || Unpin: 591 || Unpin: 592 || Unpin: 593 || Unpin: 594 || Unpin: 595 || Unpin: 596 || Unpin: 597 || Unpin: 598 || Unpin: 599 ||

// Unpin: 600 || Unpin: 601 || Unpin: 602 || Unpin: 603 || Unpin: 604 || Unpin: 605 || Unpin: 606 || Unpin: 607 || Unpin: 608 || Unpin: 609 ||
// Unpin: 610 || Unpin: 611 || Unpin: 612 || Unpin: 613 || Unpin: 614 || Unpin: 615 || Unpin: 616 || Unpin: 617 || Unpin: 618 || Unpin: 619 ||
// Unpin: 620 || Unpin: 621 || Unpin: 622 || Unpin: 623 || Unpin: 624 || Unpin: 625 || Unpin: 626 || Unpin: 627 || Unpin: 628 || Unpin: 629 ||
// Unpin: 630 || Unpin: 631 || Unpin: 632 || Unpin: 633 || Unpin: 634 || Unpin: 635 || Unpin: 636 || Unpin: 637 || Unpin: 638 || Unpin: 639 ||
// Unpin: 640 || Unpin: 641 || Unpin: 642 || Unpin: 643 || Unpin: 644 || Unpin: 645 || Unpin: 646 || Unpin: 647 || Unpin: 648 || Unpin: 649 ||
// Unpin: 650 || Unpin: 651 || Unpin: 652 || Unpin: 653 || Unpin: 654 || Unpin: 655 || Unpin: 656 || Unpin: 657 || Unpin: 658 || Unpin: 659 ||
// Unpin: 660 || Unpin: 661 || Unpin: 662 || Unpin: 663 || Unpin: 664 || Unpin: 665 || Unpin: 666 || Unpin: 667 || Unpin: 668 || Unpin: 669 ||
// Unpin: 670 || Unpin: 671 || Unpin: 672 || Unpin: 673 || Unpin: 674 || Unpin: 675 || Unpin: 676 || Unpin: 677 || Unpin: 678 || Unpin: 679 ||
// Unpin: 680 || Unpin: 681 || Unpin: 682 || Unpin: 683 || Unpin: 684 || Unpin: 685 || Unpin: 686 || Unpin: 687 || Unpin: 688 || Unpin: 689 ||
// Unpin: 690 || Unpin: 691 || Unpin: 692 || Unpin: 693 || Unpin: 694 || Unpin: 695 || Unpin: 696 || Unpin: 697 || Unpin: 698 || Unpin: 699 ||

// Unpin: 700 || Unpin: 701 || Unpin: 702 || Unpin: 703 || Unpin: 704 || Unpin: 705 || Unpin: 706 || Unpin: 707 || Unpin: 708 || Unpin: 709 ||
// Unpin: 710 || Unpin: 711 || Unpin: 712 || Unpin: 713 || Unpin: 714 || Unpin: 715 || Unpin: 716 || Unpin: 717 || Unpin: 718 || Unpin: 719 ||
// Unpin: 720 || Unpin: 721 || Unpin: 722 || Unpin: 723 || Unpin: 724 || Unpin: 725 || Unpin: 726 || Unpin: 727 || Unpin: 728 || Unpin: 729 ||
// Unpin: 730 || Unpin: 731 || Unpin: 732 || Unpin: 733 || Unpin: 734 || Unpin: 735 || Unpin: 736 || Unpin: 737 || Unpin: 738 || Unpin: 739 ||
// Unpin: 740 || Unpin: 741 || Unpin: 742 || Unpin: 743 || Unpin: 744 || Unpin: 745 || Unpin: 746 || Unpin: 747 || Unpin: 748 || Unpin: 749 ||
// Unpin: 750 || Unpin: 751 || Unpin: 752 || Unpin: 753 || Unpin: 754 || Unpin: 755 || Unpin: 756 || Unpin: 757 || Unpin: 758 || Unpin: 759 ||
// Unpin: 760 || Unpin: 761 || Unpin: 762 || Unpin: 763 || Unpin: 764 || Unpin: 765 || Unpin: 766 || Unpin: 767 || Unpin: 768 || Unpin: 769 ||
// Unpin: 770 || Unpin: 771 || Unpin: 772 || Unpin: 773 || Unpin: 774 || Unpin: 775 || Unpin: 776 || Unpin: 777 || Unpin: 778 || Unpin: 779 ||
// Unpin: 780 || Unpin: 781 || Unpin: 782 || Unpin: 783 || Unpin: 784 || Unpin: 785 || Unpin: 786 || Unpin: 787 || Unpin: 788 || Unpin: 789 ||
// Unpin: 790 || Unpin: 791 || Unpin: 792 || Unpin: 793 || Unpin: 794 || Unpin: 795 || Unpin: 796 || Unpin: 797 || Unpin: 798 || Unpin: 799 ||

// Unpin: 800 || Unpin: 801 || Unpin: 802 || Unpin: 803 || Unpin: 804 || Unpin: 805 || Unpin: 806 || Unpin: 807 || Unpin: 808 || Unpin: 809 ||
// Unpin: 810 || Unpin: 811 || Unpin: 812 || Unpin: 813 || Unpin: 814 || Unpin: 815 || Unpin: 816 || Unpin: 817 || Unpin: 818 || Unpin: 819 ||
// Unpin: 820 || Unpin: 821 || Unpin: 822 || Unpin: 823 || Unpin: 824 || Unpin: 825 || Unpin: 826 || Unpin: 827 || Unpin: 828 || Unpin: 829 ||
// Unpin: 830 || Unpin: 831 || Unpin: 832 || Unpin: 833 || Unpin: 834 || Unpin: 835 || Unpin: 836 || Unpin: 837 || Unpin: 838 || Unpin: 839 ||
// Unpin: 840 || Unpin: 841 || Unpin: 842 || Unpin: 843 || Unpin: 844 || Unpin: 845 || Unpin: 846 || Unpin: 847 || Unpin: 848 || Unpin: 849 ||
// Unpin: 850 || Unpin: 851 || Unpin: 852 || Unpin: 853 || Unpin: 854 || Unpin: 855 || Unpin: 856 || Unpin: 857 || Unpin: 858 || Unpin: 859 ||
// Unpin: 860 || Unpin: 861 || Unpin: 862 || Unpin: 863 || Unpin: 864 || Unpin: 865 || Unpin: 866 || Unpin: 867 || Unpin: 868 || Unpin: 869 ||
// Unpin: 870 || Unpin: 871 || Unpin: 872 || Unpin: 873 || Unpin: 874 || Unpin: 875 || Unpin: 876 || Unpin: 877 || Unpin: 878 || Unpin: 879 ||
// Unpin: 880 || Unpin: 881 || Unpin: 882 || Unpin: 883 || Unpin: 884 || Unpin: 885 || Unpin: 886 || Unpin: 887 || Unpin: 888 || Unpin: 889 ||
// Unpin: 890 || Unpin: 891 || Unpin: 892 || Unpin: 893 || Unpin: 894 || Unpin: 895 || Unpin: 896 || Unpin: 897 || Unpin: 898 || Unpin: 899 ||

// Unpin: 900 || Unpin: 901 || Unpin: 902 || Unpin: 903 || Unpin: 904 || Unpin: 905 || Unpin: 906 || Unpin: 907 || Unpin: 908 || Unpin: 909 ||
// Unpin: 910 || Unpin: 911 || Unpin: 912 || Unpin: 913 || Unpin: 914 || Unpin: 915 || Unpin: 916 || Unpin: 917 || Unpin: 918 || Unpin: 919 ||
// Unpin: 920 || Unpin: 921 || Unpin: 922 || Unpin: 923 || Unpin: 924 || Unpin: 925 || Unpin: 926 || Unpin: 927 || Unpin: 928 || Unpin: 929 ||
// Unpin: 930 || Unpin: 931 || Unpin: 932 || Unpin: 933 || Unpin: 934 || Unpin: 935 || Unpin: 936 || Unpin: 937 || Unpin: 938 || Unpin: 939 ||
// Unpin: 940 || Unpin: 941 || Unpin: 942 || Unpin: 943 || Unpin: 944 || Unpin: 945 || Unpin: 946 || Unpin: 947 || Unpin: 948 || Unpin: 949 ||
// Unpin: 950 || Unpin: 951 || Unpin: 952 || Unpin: 953 || Unpin: 954 || Unpin: 955 || Unpin: 956 || Unpin: 957 || Unpin: 958 || Unpin: 959 ||
// Unpin: 960 || Unpin: 961 || Unpin: 962 || Unpin: 963 || Unpin: 964 || Unpin: 965 || Unpin: 966 || Unpin: 967 || Unpin: 968 || Unpin: 969 ||
// Unpin: 970 || Unpin: 971 || Unpin: 972 || Unpin: 973 || Unpin: 974 || Unpin: 975 || Unpin: 976 || Unpin: 977 || Unpin: 978 || Unpin: 979 ||
// Unpin: 980 || Unpin: 981 || Unpin: 982 || Unpin: 983 || Unpin: 984 || Unpin: 985 || Unpin: 986 || Unpin: 987 || Unpin: 988 || Unpin: 989 ||
// Unpin: 990 || Unpin: 991 || Unpin: 992 || Unpin: 993 || Unpin: 994 || Unpin: 995 || Unpin: 996 || Unpin: 997 || Unpin: 998 || Unpin: 999 ||
//
// Victim || /autograder/submission/project-handout/test/buffer/grading_clock_replacer_test.cpp:117: Failure

}

//TEST(ClockReplacerTest, UnpinTest) {
//  constexpr size_t num_pages = 1000;
//  constexpr frame_id_t insert_times = 1200;
//  ClockReplacer clock_replacer(num_pages);
//
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Unpin(i);
//  }
//  EXPECT_EQ(num_pages, clock_replacer.Size());
//}
//
//TEST(ClockReplacerTest, PinTest) {
//  constexpr size_t num_pages = 1000;
//  constexpr frame_id_t insert_times = 1200;
//  static_assert(num_pages <= insert_times);
//  ClockReplacer clock_replacer(num_pages);
//
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Unpin(i);
//  }
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Pin(i);
//  }
//  EXPECT_EQ(0, clock_replacer.Size());
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Pin(i);
//  }
//}
//
//TEST(ClockReplacerTest, PinUnpinTest) {
//  constexpr size_t num_pages = 1000;
//  constexpr frame_id_t insert_times = 1200;
//  static_assert(num_pages <= insert_times);
//  ClockReplacer clock_replacer(num_pages);
//
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Unpin(i);
//  }
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Pin(i);
//    clock_replacer.Unpin(i);
//    clock_replacer.Pin(i);
//  }
//  EXPECT_EQ(0, clock_replacer.Size());
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Unpin(i);
//  }
//  EXPECT_EQ(num_pages, clock_replacer.Size());
//}
//
//TEST(ClockReplacerTest, VictimTest) {
//  constexpr size_t num_pages = 1000;
//  constexpr frame_id_t insert_times = 1200;
//  static_assert(num_pages <= insert_times);
//  ClockReplacer clock_replacer(num_pages);
//
//  for (frame_id_t i = 0; i < insert_times; i++) {
//    clock_replacer.Unpin(i);
//  }
//  frame_id_t frame_id;
//  for (size_t i = 0; i < num_pages; i++) {
//    EXPECT_TRUE(clock_replacer.Victim(&frame_id));
//  }
//  EXPECT_FALSE(clock_replacer.Victim(&frame_id));
//}

}  // namespace bustub
