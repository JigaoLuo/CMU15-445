//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);
  EXPECT_EQ(1, page0->GetPinCount());  // Added by Jigao
  EXPECT_EQ(1, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(i, page_id_temp);  // Added by Jigao
    EXPECT_EQ(1, bpm->GetPagePinCount(i));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(i + 1, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(INVALID_PAGE_ID, page_id_temp);  // Added by Jigao
    EXPECT_EQ(buffer_pool_size, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    EXPECT_EQ(0, bpm->GetPagePinCount(i));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(i + 1, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
    EXPECT_EQ(buffer_pool_size, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(i + buffer_pool_size, page_id_temp);  // Added by Jigao
    EXPECT_EQ(4 - i, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
    EXPECT_EQ(buffer_pool_size, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  }
  EXPECT_EQ(1, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test

  // Scenario: We should be able to fetch the data we wrote a while ago.
  EXPECT_FALSE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));
  EXPECT_TRUE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
  EXPECT_EQ(0, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
  EXPECT_EQ(buffer_pool_size, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  EXPECT_EQ(1, bpm->GetPagePinCount(0));  // Added by Jigao, GetPagePinCount is a function for test

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}


// ----------------------------------------------------------------------------------------------------
// Test from: https://github.com/xinzhu-cai/bustub/blob/master/test/buffer/buffer_pool_manager_test.cpp
// Check whether pages containing terminal characters can be recovered
// START FROM HERE
// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  // Generate random binary data
  unsigned int seed = 15645;
  for (char &i : random_binary_data) {
    i = static_cast<char>(rand_r(&seed) % 256);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::strncpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::strcmp(page0->GetData(), random_binary_data));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one cache frame left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), random_binary_data));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
// Test from: https://github.com/xinzhu-cai/bustub/blob/master/test/buffer/buffer_pool_manager_test.cpp
// END UNTIL HERE
// ----------------------------------------------------------------------------------------------------


// ---------------------------------------------------------------------------------
// Test from https://github.com/yixuaz/CMU-15445/blob/master/cmu_15445_2017(sol).rar
// START FROM HERE
TEST(BufferPoolManagerTest, SampleTest2) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);
  EXPECT_EQ(1, page0->GetPinCount());  // Added by Jigao
  EXPECT_EQ(1, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(i, page_id_temp);  // Added by Jigao
    EXPECT_EQ(1, bpm->GetPagePinCount(i));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(i + 1, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(INVALID_PAGE_ID, page_id_temp);  // Added by Jigao
    EXPECT_EQ(buffer_pool_size, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  }

  // Scenario: unpin the first page, add them to clock, set as dirty
  for (int i = 0; i < 1; ++i) {
    EXPECT_TRUE(bpm->UnpinPage(i, true));
    EXPECT_EQ(0, bpm->GetPagePinCount(i));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(1, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
    EXPECT_EQ(10, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test

    EXPECT_TRUE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
    page0 = bpm->FetchPage(0);
    EXPECT_TRUE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
    EXPECT_EQ(0, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
    EXPECT_EQ(10, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
    EXPECT_EQ(1, bpm->GetPagePinCount(i));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

    EXPECT_TRUE(bpm->UnpinPage(i, true));
    EXPECT_EQ(0, bpm->GetPagePinCount(i));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(1, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
    EXPECT_EQ(10, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test

    EXPECT_TRUE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_FALSE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
    EXPECT_EQ(10, page_id_temp);
    EXPECT_EQ(1, bpm->GetPagePinCount(10));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(0, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
  }

  std::vector<int> test{5, 6, 7, 8, 9, 10};

  for (auto v: test) {
    EXPECT_EQ(1, bpm->GetPagePinCount(v));  // Added by Jigao, GetPagePinCount is a function for test
    Page* page = bpm->FetchPage(v);
    EXPECT_EQ(2, bpm->GetPagePinCount(v));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_NE(page, nullptr);
    EXPECT_EQ(v, page->GetPageId());
    EXPECT_TRUE(bpm->UnpinPage(v, true));
    EXPECT_EQ(1, bpm->GetPagePinCount(v));  // Added by Jigao, GetPagePinCount is a function for test
    EXPECT_EQ(10, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  }

  EXPECT_EQ(0, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
  EXPECT_TRUE(bpm->UnpinPage(10, true));
  EXPECT_EQ(0, bpm->GetPagePinCount(10));  // Added by Jigao, GetPagePinCount is a function for test
  EXPECT_EQ(1, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test

  // fetch page one again
  EXPECT_FALSE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
  page0 = bpm->FetchPage(0);
  EXPECT_TRUE(bpm->FindInBuffer(0));  // Added by Jigao, FindInBuffer is a function for test
  EXPECT_EQ(0, bpm->GetReplacerSize());  // Added by Jigao, GetReplacerSize is a function for test
  EXPECT_EQ(10, bpm->GetPageTableSize());  // Added by Jigao, GetPageTableSize is a function for test
  EXPECT_EQ(1, bpm->GetPagePinCount(0));  // Added by Jigao, GetPagePinCount is a function for test

  // check read content
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
// Test from https://github.com/yixuaz/CMU-15445/blob/master/cmu_15445_2017(sol).rar
// END UNTIL HERE
// ---------------------------------------------------------------------------------


// Added by Jigao
// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, PersistenStartTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const char strings[10][PAGE_SIZE] = {"Hello", "World", "This", "Is", "A",
                                       "Persistent Start Test", "For", "Buffer Pool Manager", "In", "DBMS"};
  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  page_id_t page_id_temp;
  for (size_t i = 0; i < buffer_pool_size; ++i) {
    auto *page = bpm->NewPage(&page_id_temp);
    EXPECT_NE(nullptr, page);

    // Scenario: The buffer pool is empty. We should be able to create a new page.
    EXPECT_EQ(i, page_id_temp);

    // Scenario: Once we have a page, we should be able to read and write content.
    snprintf(page->GetData(), PAGE_SIZE, "%s", strings[i]);
    EXPECT_EQ(0, strcmp(page->GetData(), strings[i]));

    // Scenario: unpinning pages
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }

  // Scenario: Shutdown buffer pool manager
  bpm->FlushAllPages();
  delete bpm;

  // Scenario: Restart
  bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  // Scenario: We should be able to fetch the data before the shutdown
  for (size_t i = 0; i < buffer_pool_size; ++i) {
    auto *page = bpm->FetchPage(i);
    EXPECT_EQ(0, strcmp(page->GetData(), strings[i]));
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
