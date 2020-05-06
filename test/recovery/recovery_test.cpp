//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// recovery_test.cpp
//
// Identification: test/execution/recovery_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "common/bustub_instance.h"
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"
#include "logging/common.h"
#include "recovery/log_recovery.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

// Added by Jigao
// Idea from CMU Fall2017 Project, modified to fit Fall 2019 Code
// NOLINTNEXTLINE
TEST(LogManagerTest, BasicLogging) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  LOG_INFO("Insert and delete a random tuple");

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  ASSERT_TRUE(test_table->InsertTuple(tuple1, &rid1, txn));

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Commit txn");

  bustub_instance->log_manager_->StopFlushThread();
  ASSERT_FALSE(enable_logging);
  LOG_INFO("Turning off flushing thread");

  // some basic manually checking here
  char buffer[PAGE_SIZE];
  bustub_instance->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
  int32_t size = *reinterpret_cast<int32_t *>(buffer);
  lsn_t lsn_ = *reinterpret_cast<lsn_t *>(buffer + 4);
  txn_id_t txn_id_ = *reinterpret_cast<txn_id_t *>(buffer + 4 + 4);
  lsn_t prevLSN = *reinterpret_cast<lsn_t *>(buffer + 4 + 4 + 4);
  LogRecordType log_record_type = *reinterpret_cast<LogRecordType *>(buffer + 4 + 4 + 4 + 4);
  ASSERT_EQ(20, size);  // LogRecordType::BEGIN
  ASSERT_EQ(0, lsn_);
  ASSERT_EQ(txn->GetTransactionId(), txn_id_);
  ASSERT_EQ(INVALID_LSN, prevLSN);
  ASSERT_EQ(LogRecordType::BEGIN, log_record_type);
  LOG_INFO("LogRecordType::BEGIN size  = %d", size);

  size = *reinterpret_cast<int32_t *>(buffer + 20);
  lsn_ = *reinterpret_cast<lsn_t *>(buffer + 20 + 4);
  txn_id_ = *reinterpret_cast<txn_id_t *>(buffer + 20 + 4 + 4);
  prevLSN = *reinterpret_cast<lsn_t *>(buffer + 20 + 4 + 4 + 4);
  log_record_type = *reinterpret_cast<LogRecordType *>(buffer + 20 + 4 + 4 + 4 + 4);
  ASSERT_EQ(28, size);  // LogRecordType::NEWPAGE
  ASSERT_EQ(1, lsn_);
  ASSERT_EQ(txn->GetTransactionId(), txn_id_);
  ASSERT_EQ(0, prevLSN);
  ASSERT_EQ(LogRecordType::NEWPAGE, log_record_type);
  LOG_INFO("LogRecordType::NEWPAGE size  = %d", size);

  int32_t tuple1_size = 0;
  tuple1_size = *reinterpret_cast<int32_t *>(buffer + 48);
  lsn_ = *reinterpret_cast<lsn_t *>(buffer + 48 + 4);
  txn_id_ = *reinterpret_cast<txn_id_t *>(buffer + 48 + 4 + 4);
  prevLSN = *reinterpret_cast<lsn_t *>(buffer + 48 + 4 + 4 + 4);
  log_record_type = *reinterpret_cast<LogRecordType *>(buffer + 48 + 4 + 4 + 4 + 4);
  ASSERT_EQ(2, lsn_);
  ASSERT_EQ(txn->GetTransactionId(), txn_id_);
  ASSERT_EQ(1, prevLSN);
  ASSERT_EQ(LogRecordType::INSERT, log_record_type);
  LOG_INFO("LogRecordType::INSERT tuple1_size  = %d", tuple1_size);  // LogRecordType::INSERT for tuple 1 => not fix-sized

  int32_t tuple2_size = 0;
  tuple2_size = *reinterpret_cast<int32_t *>(buffer + 48 + tuple1_size);
  lsn_ = *reinterpret_cast<lsn_t *>(buffer + 48 + tuple1_size + 4);
  txn_id_ = *reinterpret_cast<txn_id_t *>(buffer + 48 + tuple1_size + 4 + 4);
  prevLSN = *reinterpret_cast<lsn_t *>(buffer + 48 + tuple1_size + 4 + 4 + 4);
  log_record_type = *reinterpret_cast<LogRecordType *>(buffer + 48 + tuple1_size + 4 + 4 + 4 + 4);
  ASSERT_EQ(3, lsn_);
  ASSERT_EQ(txn->GetTransactionId(), txn_id_);
  ASSERT_EQ(2, prevLSN);
  ASSERT_EQ(LogRecordType::INSERT, log_record_type);
  LOG_INFO("LogRecordType::INSERT tuple2_size  = %d", tuple2_size);  // LogRecordType::INSERT for tuple 2 => not fix-sized

  size = *reinterpret_cast<int32_t *>(buffer + 48 + tuple1_size + tuple2_size);
  lsn_ = *reinterpret_cast<lsn_t *>(buffer + 48 + tuple1_size + tuple2_size + 4);
  txn_id_ = *reinterpret_cast<txn_id_t *>(buffer + 48 + tuple1_size + tuple2_size + 4 + 4);
  prevLSN = *reinterpret_cast<lsn_t *>(buffer + 48 + tuple1_size + tuple2_size + 4 + 4 + 4);
  log_record_type = *reinterpret_cast<LogRecordType *>(buffer + 48 + tuple1_size + tuple2_size + 4 + 4 + 4 + 4);
  ASSERT_EQ(4, lsn_);
  ASSERT_EQ(20, size);  // LogRecordType::COMMIT
  ASSERT_EQ(txn->GetTransactionId(), txn_id_);
  ASSERT_EQ(3, prevLSN);
  ASSERT_EQ(LogRecordType::COMMIT, log_record_type);
  LOG_INFO("LogRecordType::COMMIT size  = %d", size);

  delete txn;
  delete test_table;
  delete bustub_instance;
  LOG_INFO("Teared down the system");
  remove("test.db");
  remove("test.log");
}

// --------------------------------------------------------
// Test from https://github.com/yixuaz/CMU-15445/blob/master/project4/test/log_manager_test.cpp
// STARTS

txn_id_t StartTransaction(BustubInstance *bustub_instance, TableHeap* test_table) {
  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  LOG_INFO("Insert and delete a random tuple");

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  RID rid;
  EXPECT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  EXPECT_TRUE(test_table->MarkDelete(rid, txn));

  LOG_INFO("Commit txn %d", txn->GetTransactionId());
  bustub_instance->transaction_manager_->Commit(txn);
  txn_id_t result = txn->GetTransactionId();
  delete txn;
  return result;
}


void StartTransaction1(BustubInstance *bustub_instance, TableHeap* test_table) {
  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  LOG_INFO("Insert and delete a random tuple");

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  for (int i = 0; i < 10; i++) {
    const Tuple tuple = ConstructTuple(&schema);
    RID rid;
    ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  }
  LOG_INFO("Commit txn %d", txn->GetTransactionId());
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
}

TEST(LogManagerTest, LoggingWithGroupCommit) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                        bustub_instance->log_manager_, txn);

  LOG_INFO("Insert and delete a random tuple");

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  RID rid;
  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  ASSERT_TRUE(test_table->MarkDelete(rid, txn));

  LOG_INFO("Commit txn %d", txn->GetTransactionId());
  bustub_instance->transaction_manager_->Commit(txn);

  std::future<txn_id_t> fut1 = std::async(std::launch::async, StartTransaction, bustub_instance, test_table);
  std::future<txn_id_t> fut2 = std::async(std::launch::async, StartTransaction, bustub_instance, test_table);
  std::future<txn_id_t> fut3 = std::async(std::launch::async, StartTransaction, bustub_instance, test_table);

  txn_id_t txn_id_1 = fut1.get();
  txn_id_t txn_id_2 = fut2.get();
  txn_id_t txn_id_3 = fut3.get();
  std::vector<txn_id_t> txn_ids{txn_id_1, txn_id_2, txn_id_3};

  bustub_instance->log_manager_->StopFlushThread();
  ASSERT_FALSE(enable_logging);
  LOG_INFO("Turning off flushing thread");

  // some basic manually checking here
  char buffer[PAGE_SIZE];
  bustub_instance->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
  LogRecord* log_record = reinterpret_cast<LogRecord *>(buffer);
  ASSERT_EQ(20, log_record->GetSize());  // LogRecordType::BEGIN
  ASSERT_EQ(0, log_record->GetLSN());
  ASSERT_EQ(txn->GetTransactionId(), log_record->GetTxnId());
  ASSERT_EQ(INVALID_LSN, log_record->GetPrevLSN());
  ASSERT_EQ(LogRecordType::BEGIN, log_record->GetLogRecordType());
  LOG_INFO("LogRecordType::BEGIN size  = %d", log_record->GetSize());

  log_record = reinterpret_cast<LogRecord *>(buffer + 20);
  ASSERT_EQ(28, log_record->GetSize());  // LogRecordType::NEWPAGE
  ASSERT_EQ(1, log_record->GetLSN());
  ASSERT_EQ(txn->GetTransactionId(), log_record->GetTxnId());
  ASSERT_EQ(0, log_record->GetPrevLSN());
  ASSERT_EQ(LogRecordType::NEWPAGE, log_record->GetLogRecordType());
  LOG_INFO("LogRecordType::NEWPAGE size  = %d", log_record->GetSize());

  LogRecord* log_record_tuple1 = reinterpret_cast<LogRecord *>(buffer + 48);
  ASSERT_EQ(2, log_record_tuple1->GetLSN());
  ASSERT_EQ(txn->GetTransactionId(), log_record_tuple1->GetTxnId());
  ASSERT_EQ(1, log_record_tuple1->GetPrevLSN());
  ASSERT_EQ(LogRecordType::INSERT, log_record_tuple1->GetLogRecordType());
  const auto &insert_tuple1_rid = log_record_tuple1->GetInsertRID();
  auto page_id = insert_tuple1_rid.GetPageId();
  ASSERT_NE(INVALID_PAGE_ID, page_id);
  LOG_INFO("LogRecordType::INSERT tuple1_size  = %d", log_record_tuple1->GetSize());  // LogRecordType::INSERT for tuple 1 => not fix-sized

  log_record = reinterpret_cast<LogRecord *>(buffer + 48 + log_record_tuple1->GetSize());
  ASSERT_EQ(32, log_record->GetSize());  // LogRecordType::MARKDELETE
  ASSERT_EQ(3, log_record->GetLSN());
  ASSERT_EQ(txn->GetTransactionId(), log_record->GetTxnId());
  ASSERT_EQ(2, log_record->GetPrevLSN());
  ASSERT_EQ(LogRecordType::MARKDELETE, log_record->GetLogRecordType());
  LOG_INFO("LogRecordType::MARKDELETE size  = %d", log_record->GetSize());  // LogRecordType::MARKDELETE for tuple 1

  log_record = reinterpret_cast<LogRecord *>(buffer + 48 + log_record_tuple1->GetSize() + 32);
  ASSERT_EQ(log_record_tuple1->GetSize(), log_record->GetSize());  // LogRecordType::APPLYDELETE
  ASSERT_EQ(4, log_record->GetLSN());
  ASSERT_EQ(txn->GetTransactionId(), log_record->GetTxnId());
  ASSERT_EQ(3, log_record->GetPrevLSN());
  ASSERT_EQ(LogRecordType::APPLYDELETE, log_record->GetLogRecordType());
  LOG_INFO("LogRecordType::APPLYDELETE tuple1_size  = %d", log_record->GetSize());  // LogRecordType::APPLYDELETE for tuple 1 => not fix-sized

  log_record = reinterpret_cast<LogRecord *>(buffer + 48 + log_record_tuple1->GetSize() + 32 + log_record_tuple1->GetSize());
  ASSERT_EQ(20, log_record->GetSize());  // LogRecordType::COMMIT
  ASSERT_EQ(5, log_record->GetLSN());
  ASSERT_EQ(txn->GetTransactionId(), log_record->GetTxnId());
  ASSERT_EQ(4, log_record->GetPrevLSN());
  ASSERT_EQ(LogRecordType::COMMIT, log_record->GetLogRecordType());
  LOG_INFO("LogRecordType::COMMIT size  = %d", log_record->GetSize());

  const size_t offset = 48 + log_record_tuple1->GetSize() + 32 + log_record_tuple1->GetSize() + 20;
  const lsn_t lsn = 5;
  size_t txn_id_match_counter = 0;
  for (const auto& txn_id : txn_ids) {
    size_t local_offset = offset;
    lsn_t global_lsn = lsn;
    LOG_INFO("Transaction Id  = %d", txn_id);

    // LogRecordType::BEGIN
    LogRecord* local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    while (txn_id != local_log_record->GetTxnId()) {
      ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
      local_offset += local_log_record->GetSize();
      local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    }
    ASSERT_EQ(20, local_log_record->GetSize());  // LogRecordType::BEGIN
    ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
    ASSERT_EQ(INVALID_LSN, local_log_record->GetPrevLSN());
    ASSERT_EQ(LogRecordType::BEGIN, local_log_record->GetLogRecordType());
    LOG_INFO("LogRecordType::BEGIN size  = %d", local_log_record->GetSize());
    lsn_t prev_lsn = global_lsn;
    local_offset += 20;

    // LogRecordType::INSERT
    local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    while (txn_id != local_log_record->GetTxnId()) {
      ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
      local_offset += local_log_record->GetSize();
      local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    }
    LogRecord* local_log_record_tuple1 = reinterpret_cast<LogRecord *>(buffer + local_offset);
    ASSERT_EQ(++global_lsn, local_log_record_tuple1->GetLSN());
    ASSERT_EQ(prev_lsn, local_log_record_tuple1->GetPrevLSN());
    ASSERT_EQ(LogRecordType::INSERT, local_log_record_tuple1->GetLogRecordType());
    const auto &local_insert_tuple1_rid = local_log_record_tuple1->GetInsertRID();
    page_id = local_insert_tuple1_rid.GetPageId();
    ASSERT_NE(INVALID_PAGE_ID, page_id);
    LOG_INFO("LogRecordType::INSERT tuple1_size  = %d", local_log_record_tuple1->GetSize());  // LogRecordType::INSERT for tuple 1 => not fix-sized
    prev_lsn = global_lsn;
    local_offset += local_log_record_tuple1->GetSize();

    // LogRecordType::MARKDELETE
    local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    while (txn_id != local_log_record->GetTxnId()) {
      ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
      local_offset += local_log_record->GetSize();
      local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    }
    ASSERT_EQ(32, local_log_record->GetSize());  // LogRecordType::MARKDELETE
    ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
    ASSERT_EQ(prev_lsn, local_log_record->GetPrevLSN());
    ASSERT_EQ(LogRecordType::MARKDELETE, local_log_record->GetLogRecordType());
    LOG_INFO("LogRecordType::MARKDELETE size  = %d", local_log_record->GetSize());  // LogRecordType::MARKDELETE for tuple 1
    prev_lsn = global_lsn;
    local_offset += 32;

    // LogRecordType::APPLYDELETE
    local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    while (txn_id != local_log_record->GetTxnId()) {
      ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
      local_offset += local_log_record->GetSize();
      local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    }
    ASSERT_EQ(local_log_record_tuple1->GetSize(), local_log_record->GetSize());  // LogRecordType::APPLYDELETE
    ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
    ASSERT_EQ(prev_lsn, local_log_record->GetPrevLSN());
    ASSERT_EQ(LogRecordType::APPLYDELETE, local_log_record->GetLogRecordType());
    LOG_INFO("LogRecordType::APPLYDELETE tuple1_size  = %d", local_log_record->GetSize());  // LogRecordType::APPLYDELETE for tuple 1 => not fix-sized
    prev_lsn = global_lsn;
    local_offset += local_log_record->GetSize();

    // LogRecordType::COMMIT
    local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    while (txn_id != local_log_record->GetTxnId()) {
      ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
      local_offset += local_log_record->GetSize();
      local_log_record = reinterpret_cast<LogRecord *>(buffer + local_offset);
    }
    ASSERT_EQ(20, local_log_record->GetSize());  // LogRecordType::COMMIT
    ASSERT_EQ(++global_lsn, local_log_record->GetLSN());
    ASSERT_EQ(prev_lsn, local_log_record->GetPrevLSN());
    ASSERT_EQ(LogRecordType::COMMIT, local_log_record->GetLogRecordType());
    LOG_INFO("LogRecordType::COMMIT size  = %d", local_log_record->GetSize());

    txn_id_match_counter++;
  }

  ASSERT_EQ(3, txn_id_match_counter);

  delete txn;
  delete test_table;
  delete bustub_instance;
  LOG_INFO("Teared down the system");
  remove("test.db");
  remove("test.log");
}

TEST(LogManagerTest, SingleLoggingWithBufferFull) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                        bustub_instance->log_manager_, txn);

  LOG_INFO("Insert and delete a random tuple");

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};

  for (int i = 0; i < 13; i++) {
    const Tuple tuple = ConstructTuple(&schema);
    RID rid;
    ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  }
  LOG_INFO("Commit txn %d", txn->GetTransactionId());
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  bustub_instance->log_manager_->StopFlushThread();
  EXPECT_FALSE(enable_logging);
  LOG_INFO("Turning off flushing thread");
  LOG_INFO("num of flushes = %d", bustub_instance->disk_manager_->GetNumFlushes());

  delete test_table;
  delete bustub_instance;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}

TEST(LogManagerTest, MultiLoggingWithBufferFull) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                        bustub_instance->log_manager_, txn);

  LOG_INFO("Insert and delete a random tuple");

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};

  for (int i = 0; i < 13; i++) {
    const Tuple tuple = ConstructTuple(&schema);
    RID rid;
    ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  }
  LOG_INFO("Commit txn %d", txn->GetTransactionId());
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  std::future<void> fut1 = std::async(std::launch::async, StartTransaction1, bustub_instance, test_table);
  std::future<void> fut2 = std::async(std::launch::async, StartTransaction1, bustub_instance, test_table);

  fut1.get();
  fut2.get();

  bustub_instance->log_manager_->StopFlushThread();
  EXPECT_FALSE(enable_logging);
  LOG_DEBUG("Turning off flushing thread");


  LOG_DEBUG("num of flushes = %d", bustub_instance->disk_manager_->GetNumFlushes());

  delete test_table;
  delete bustub_instance;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}

// Test from https://github.com/yixuaz/CMU-15445/blob/master/project4/test/log_manager_test.cpp
// ENDS
// --------------------------------------------------------


// NOLINTNEXTLINE
TEST(RecoveryTest, RedoTest) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);

  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val1_0 = tuple1.GetValue(&schema, 0);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  ASSERT_TRUE(test_table->InsertTuple(tuple1, &rid1, txn));

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Commit txn");

  sleep(2);
  LOG_INFO("SLEEPING for 2s");

  delete txn;
  delete test_table;

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("System restart...");
  bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Check if tuple is not in table before recovery");
  Tuple old_tuple;
  Tuple old_tuple1;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_FALSE(test_table->GetTuple(rid1, &old_tuple1, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Begin recovery");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  LOG_INFO("Redo underway...");
  log_recovery->Redo();
  LOG_INFO("Undo underway...");
  log_recovery->Undo();

  LOG_INFO("Check if recovery success");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_TRUE(test_table->GetTuple(rid1, &old_tuple1, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 1).CompareEquals(val1_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 0).CompareEquals(val1_0), CmpBool::CmpTrue);

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, UndoTest) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  RID rid;
  const Tuple tuple = ConstructTuple(&schema);

  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));

  LOG_INFO("Table page content is written to disk");
  bustub_instance->buffer_pool_manager_->FlushPage(first_page_id);

  sleep(2);
  LOG_INFO("SLEEPING for 2s");

  delete txn;
  delete test_table;

  LOG_INFO("System crash before commit");
  delete bustub_instance;

  LOG_INFO("System restarted..");
  bustub_instance = new BustubInstance("test.db");

  LOG_INFO("Check if tuple exists before recovery");
  Tuple old_tuple;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Recovery started..");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  LOG_INFO("Check if failed txn is undo successfully");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  bustub_instance->transaction_manager_->Commit(txn);

  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, BasicRedoTestWithOneTxn) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  RID rid1;
  RID rid2;
  RID rid3;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val1_0 = tuple1.GetValue(&schema, 0);
  auto val2_1 = tuple2.GetValue(&schema, 1);
  auto val2_0 = tuple2.GetValue(&schema, 0);
  auto val3_1 = tuple3.GetValue(&schema, 1);
  auto val3_0 = tuple3.GetValue(&schema, 0);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid1, txn));
  LOG_INFO("Insert tuple");

  ASSERT_TRUE(test_table->UpdateTuple(tuple, rid1, txn));
  LOG_INFO("Update tuple to tuple1");

  ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid2, txn));
  LOG_INFO("Insert tuple2");

  ASSERT_TRUE(test_table->MarkDelete(rid2, txn));
  LOG_INFO("Delete tuple2");

  ASSERT_TRUE(test_table->InsertTuple(tuple3, &rid3, txn));
  LOG_INFO("Insert tuple3");

  sleep(2);
  LOG_INFO("SLEEPING for 2s");

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Commit txn");

  delete txn;
  delete test_table;

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("System restart...");
  bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Check if tuple is not in table before recovery");
  Tuple old_tuple1;
  Tuple old_tuple2;
  Tuple old_tuple3;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  ASSERT_FALSE(test_table->GetTuple(rid1, &old_tuple1, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple2, txn));
  ASSERT_FALSE(test_table->GetTuple(rid3, &old_tuple3, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Begin recovery");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  LOG_INFO("Redo underway...");
  log_recovery->Redo();
  LOG_INFO("Undo underway...");
  log_recovery->Undo();

  LOG_INFO("Check if recovery success");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid1, &old_tuple1, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple2, txn));
  ASSERT_TRUE(test_table->GetTuple(rid3, &old_tuple3, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  ASSERT_EQ(old_tuple1.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple3.GetValue(&schema, 1).CompareEquals(val3_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple3.GetValue(&schema, 0).CompareEquals(val3_0), CmpBool::CmpTrue);

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, BasicUndoTestWithOneTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  RID rid1;
  RID rid2;
  RID rid3;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val1_0 = tuple1.GetValue(&schema, 0);
  auto val2_1 = tuple2.GetValue(&schema, 1);
  auto val2_0 = tuple2.GetValue(&schema, 0);
  auto val3_1 = tuple3.GetValue(&schema, 1);
  auto val3_0 = tuple3.GetValue(&schema, 0);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid1, txn));
  LOG_INFO("Insert tuple");

  ASSERT_TRUE(test_table->UpdateTuple(tuple, rid1, txn));
  LOG_INFO("Update tuple to tuple1");

  ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid2, txn));
  LOG_INFO("Insert tuple2");

  ASSERT_TRUE(test_table->MarkDelete(rid2, txn));
  LOG_INFO("Delete tuple2");

  ASSERT_TRUE(test_table->InsertTuple(tuple3, &rid3, txn));
  LOG_INFO("Insert tuple3");

  LOG_INFO("Table page content is written to disk");
  bustub_instance->buffer_pool_manager_->FlushPage(first_page_id);

  sleep(2);
  LOG_INFO("SLEEPING for 2s");

  delete txn;
  delete test_table;

  LOG_INFO("System crash before commit");
  delete bustub_instance;

  LOG_INFO("System restarted..");
  bustub_instance = new BustubInstance("test.db");

  LOG_INFO("Check if tuple exists before recovery");
  Tuple old_tuple;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  Tuple old_tuple1;
  Tuple old_tuple2;
  Tuple old_tuple3;

  ASSERT_TRUE(test_table->GetTuple(rid1, &old_tuple1, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple2, txn));
  ASSERT_TRUE(test_table->GetTuple(rid3, &old_tuple3, txn));
  ASSERT_EQ(old_tuple1.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple3.GetValue(&schema, 1).CompareEquals(val3_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple3.GetValue(&schema, 0).CompareEquals(val3_0), CmpBool::CmpTrue);

  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Recovery started..");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  LOG_INFO("Check if failed txn is undo successfully");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_FALSE(test_table->GetTuple(rid1, &old_tuple1, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple2, txn));
  ASSERT_FALSE(test_table->GetTuple(rid3, &old_tuple3, txn));
  bustub_instance->transaction_manager_->Commit(txn);

  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, CheckpointTest) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  EXPECT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  EXPECT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  bustub_instance->transaction_manager_->Commit(txn);

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};

  Tuple tuple = ConstructTuple(&schema);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);

  // set log time out very high so that flush doesn't happen before checkpoint is performed
  log_timeout = std::chrono::seconds(15);

  // insert a ton of tuples
  Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
  for (int i = 0; i < 1000; i++) {
    RID rid;
    EXPECT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
  }
  bustub_instance->transaction_manager_->Commit(txn1);

  // Do checkpoint
  bustub_instance->checkpoint_manager_->BeginCheckpoint();
  bustub_instance->checkpoint_manager_->EndCheckpoint();

  Page *pages = bustub_instance->buffer_pool_manager_->GetPages();
  size_t pool_size = bustub_instance->buffer_pool_manager_->GetPoolSize();

  // make sure that all pages in the buffer pool are marked as non-dirty
  bool all_pages_clean = true;
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID && page->IsDirty()) {
      all_pages_clean = false;
      break;
    }
  }
  EXPECT_TRUE(all_pages_clean);

  // compare each page in the buffer pool to that page's
  // data on disk. ensure they match after the checkpoint
  bool all_pages_match = true;
  auto *disk_data = new char[PAGE_SIZE];
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID) {
      bustub_instance->disk_manager_->ReadPage(page_id, disk_data);
      if (std::memcmp(disk_data, page->GetData(), PAGE_SIZE) != 0) {
        all_pages_match = false;
        break;
      }
    }
  }

  EXPECT_TRUE(all_pages_match);
  delete[] disk_data;

  // Verify all committed transactions flushed to disk
  lsn_t persistent_lsn = bustub_instance->log_manager_->GetPersistentLSN();
  lsn_t next_lsn = bustub_instance->log_manager_->GetNextLSN();
  EXPECT_EQ(persistent_lsn, (next_lsn - 1));

  // verify log was flushed and each page's LSN <= persistent lsn
  bool all_pages_lte = true;
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID && page->GetLSN() > persistent_lsn) {
      all_pages_lte = false;
      break;
    }
  }

  EXPECT_TRUE(all_pages_lte);

  delete txn;
  delete txn1;
  delete test_table;

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
