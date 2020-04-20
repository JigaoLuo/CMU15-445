//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/simple_catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, zipCreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new SimpleCatalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  ASSERT_TRUE(table_metadata);
  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());
  auto table_oid = table_metadata->oid_;

  table_metadata = catalog->GetTable(table_oid);
  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(table_oid, table_metadata->oid_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());

  table_metadata = catalog->GetTable(table_name);
  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(table_oid, table_metadata->oid_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
