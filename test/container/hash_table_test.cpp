//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "common/logger.h"
#include "container/hash/linear_probe_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  // look for a key that does not exist
  std::vector<int> res;
  EXPECT_FALSE(ht.GetValue(nullptr, 20, &res));
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }
  disk_manager->ShutDown();
  remove("test.db");
  delete bpm;
  delete disk_manager;
}

// Added by Jigao
// NOLINTNEXTLINE
TEST(HashTableTest, HashTableFullTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  using Local_MappingType = std::pair<int, int>;
  const size_t LOCAL_BLOCK_ARRAY_SIZE = (4 * PAGE_SIZE / (4 * sizeof(Local_MappingType) + 1));

  // force to have two block pages
  LinearProbeHashTable<int, int, IntComparator> ht
    ("blah", bpm, IntComparator(), LOCAL_BLOCK_ARRAY_SIZE + 1, HashFunction<int>());

  // insert values until pages full
  for (int i = 0; i < static_cast<int>(LOCAL_BLOCK_ARRAY_SIZE + 1); i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  // pages full
  for (int i = 0; i < 10; i++) {
    EXPECT_THROW(ht.Insert(nullptr,
                           LOCAL_BLOCK_ARRAY_SIZE + 1 + i,
                           LOCAL_BLOCK_ARRAY_SIZE + 1 + i),
                 hash_table_full_error);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete bpm;
  delete disk_manager;
}

// Added by Jigao
// NOLINTNEXTLINE
TEST(HashTableTest, HashTableRemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht
      ("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert values until pages full
  for (int i = 0; i < 1000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  // pages full
  for (int i = 0; i < 10; i++) {
    EXPECT_THROW(ht.Insert(nullptr, 1000 + 1 + i, 1000 + 1 + i), hash_table_full_error);
  }

  // delete first half values
  for (int i = 0; i < 1000 / 2; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i * 2));
  }

  // check the deleted values
  for (int i = 0; i < 1000 / 2; i++) {
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size());
  }

  // check the existing values
  for (int i = 1000 / 2; i < 1000; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(2 * i, res[0]);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete bpm;
  delete disk_manager;
}


// Added by Jigao
// NOLINTNEXTLINE
TEST(HashTableTest, HashTableMixdedTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht
      ("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert values until pages full
  for (int i = 0; i < 1000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  // pages full
  for (int i = 0; i < 10; i++) {
    EXPECT_THROW(ht.Insert(nullptr, 1000 + 1 + i, 1000 + 1 + i), hash_table_full_error);
  }

  // delete even values
  for (int i = 0; i < 1000; i++) {
    if (i % 2 == 0) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i * 2));
    }
  }

  // check the deleted values
  for (int i = 0; i < 1000; i++) {
    if (i % 2 == 0) {
      std::vector<int> res;
      EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
      EXPECT_EQ(0, res.size());
    }
  }

  // check the existing values
  for (int i = 1000 / 2; i < 1000; i++) {
    if (i % 2 != 0) {
      std::vector<int> res;
      EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  // insert values until pages full
  for (int i = 1000; i < 1500; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  // pages full
  for (int i = 0; i < 10; i++) {
    EXPECT_THROW(ht.Insert(nullptr, 1500 + 1 + i, 1500 + 1 + i), hash_table_full_error);
  }

  // check the existing values
  for (int i = 1000 / 2; i < 1000; i++) {
    if (i % 2 != 0) {
      std::vector<int> res;
      EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }
  for (int i = 1000; i < 1500; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete bpm;
  delete disk_manager;
}

// Added by Jigao
// NOLINTNEXTLINE
TEST(HashTableTest, HashTableResizeTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht
      ("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert values until pages full
  for (int i = 0; i < 1000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  // pages full
  for (int i = 0; i < 10; i++) {
    EXPECT_THROW(ht.Insert(nullptr, 1000 + 1 + i, 1000 + 1 + i), hash_table_full_error);
  }

  // resize
  ht.Resize(1000);

  // check value after resizing
  for (int i = 0; i < 1000; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  // insert values until pages full
  for (int i = 1000; i < 2000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  // pages full
  for (int i = 0; i < 10; i++) {
    EXPECT_THROW(ht.Insert(nullptr, 2000 + 1 + i, 2000 + 1 + i), hash_table_full_error);
  }

  // check value
  for (int i = 0; i < 2000; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(2 * i, res[0]);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete bpm;
  delete disk_manager;
}

// https://github.com/astronaut0131/bustub/blob/master/test/container/hash_table_test.cpp
TEST(HashTableTest, ConcurrentTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);
  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());
  int num_threads = 1000;
  std::vector<std::thread> threads;
  for (int tid = 0; tid < num_threads; tid++) {
    threads.push_back(std::thread([tid, &ht]() {
      EXPECT_TRUE(ht.Insert(nullptr, tid, tid));
    }));
  }
  for (int tid = 0; tid < num_threads; tid++) {
    threads[tid].join();
  }

  for (int tid = 0; tid < num_threads; tid++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, tid, &res));
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], tid);
  }
  threads.clear();

  for (int tid = 0; tid < num_threads; tid++) {
    threads.push_back(std::thread([tid, &ht]() {
      EXPECT_TRUE(ht.Remove(nullptr, tid, tid));
    }));
  }
  for (int tid = 0; tid < num_threads; tid++) {
    threads[tid].join();
  }
  for (int tid = 0; tid < num_threads; tid++) {
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, tid, &res));
    EXPECT_EQ(res.size(), 0);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
