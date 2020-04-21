//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tmp_tuple_page_test.cpp
//
// Identification: test/storage/tmp_tuple_page_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "gtest/gtest.h"
#include "storage/page/tmp_tuple_page.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(TmpTuplePageTest, BasicTest) {
  // There are many ways to do this assignment, and this is only one of them.
  // If you don't like the TmpTuplePage idea, please feel free to delete this test case entirely.
  // You will get full credit as long as you are correctly using a linear probe hash table.

  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  Schema schema(columns);

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(123));

  Tuple tuple(values, &schema);
  TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
  ASSERT_TRUE(page.Insert(tuple, &tmp_tuple));

  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 8);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 8), 4);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 4), 123);
  ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
  ASSERT_EQ(tmp_tuple.GetOffset(), PAGE_SIZE - 8);
}

// Added by Jigao
// Logic printed out from gradescope
// NOLINTNEXTLINE
TEST(TmpTuplePageTest, BasicTest2) {
  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::BIGINT);
  Schema schema(columns);

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetBigIntValue(1958505087099));

  Tuple tuple(values, &schema);
  TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
  ASSERT_TRUE(page.Insert(tuple, &tmp_tuple));

  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 12);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 12), 8);
  ASSERT_EQ(*reinterpret_cast<uint64_t *>(data + PAGE_SIZE - 8), 1958505087099);
  ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
  ASSERT_EQ(tmp_tuple.GetOffset(), PAGE_SIZE - 12);
}

// Added by Jigao
// Logic printed out from gradescope
// NOLINTNEXTLINE
TEST(TmpTuplePageTest, AdvancedTest) {
  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::BIGINT);
  Schema schema(columns);

  const uint64_t num = 1954210119695;
  for (size_t i = 0; i < 300; i++) {
    std::vector<Value> values;
    values.emplace_back(ValueFactory::GetBigIntValue(num * i));

    Tuple tuple(values, &schema);
    TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
    ASSERT_TRUE(page.Insert(tuple, &tmp_tuple));

    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 12 * (i + 1));
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 12 * (i + 1)), 8);
    ASSERT_EQ(*reinterpret_cast<uint64_t *>(data + PAGE_SIZE - 12 * (i + 1) + 4), num * i);
    ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
    ASSERT_EQ(tmp_tuple.GetOffset(), PAGE_SIZE - 12 * (i + 1));
  }
}

// Added by Jigao
// Logic printed out from gradescope
// NOLINTNEXTLINE
TEST(TmpTuplePageTest, EvilTest) {
  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::INTEGER);
  columns.emplace_back("C", TypeId::SMALLINT);
  columns.emplace_back("D", TypeId::BOOLEAN);
  Schema schema(columns);

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetBigIntValue(0));
  values.emplace_back(ValueFactory::GetIntegerValue(0));
  values.emplace_back(ValueFactory::GetSmallIntValue(0));
  values.emplace_back(ValueFactory::GetBooleanValue(false));

  Tuple tuple(values, &schema);
  TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
  for (size_t i = 0; i < 214; i++) {
    ASSERT_TRUE(page.Insert(tuple, &tmp_tuple));
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 19 * (i + 1));
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 19 * (i + 1)), 15);
    ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
    ASSERT_EQ(tmp_tuple.GetOffset(), PAGE_SIZE - 19 * (i + 1));
  }
  ASSERT_FALSE(page.Insert(tuple, &tmp_tuple));
}

}  // namespace bustub
