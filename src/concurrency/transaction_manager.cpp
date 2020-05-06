//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <unordered_map>
#include <unordered_set>

#include "storage/table/table_heap.h"

namespace bustub {

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

Transaction *TransactionManager::Begin(Transaction *txn) {
  // Acquire the global transaction latch in shared mode.
  global_txn_latch_.RLock();
  if (txn == nullptr) {
    txn = new Transaction(next_txn_id_++);
  }
  if (enable_logging) {
    // TODO(student): Add logging here.
    assert(txn->GetPrevLSN() == INVALID_LSN);
    LogRecord log_record = LogRecord(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::BEGIN);
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
    txn->SetPrevLSN(lsn);
  }
  txn_map[txn->GetTransactionId()] = txn;
  return txn;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);
  // Perform all deletes before we commit.
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      // Note that this also releases the lock when holding the page latch.
      table->ApplyDelete(item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();
  if (enable_logging) {
    // TODO(student): add logging here
    // Within TransactionManager, whenever you call the Commit or Abort method,
    // you need to make sure your log records are permanently stored on disk file before releasing the locks.
    // But instead of forcing a flush, you need to wait for log_timeout or other operations
    // to implicitly trigger the flush operations.
    LogRecord log_record = LogRecord(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::COMMIT);
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
    txn->SetPrevLSN(lsn);
    log_manager_->Flush(false);
//    while (log_manager_->GetPersistentLSN() >= lsn) {
//      std::this_thread::yield();
//    }
  }
  // Release all the locks.
  ReleaseLocks(txn);
  // Release the global transaction latch.
  global_txn_latch_.RUnlock();
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);

  // Rollback before releasing the lock.
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      table->RollbackDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      // Note that this also releases the lock when holding the page latch.
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (enable_logging) {
    // TODO(student): add logging here
    // Within TransactionManager, whenever you call the Commit or Abort method,
    // you need to make sure your log records are permanently stored on disk file before releasing the locks.
    // But instead of forcing a flush, you need to wait for log_timeout or other operations
    // to implicitly trigger the flush operations.
    LogRecord log_record = LogRecord(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::ABORT);
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
    txn->SetPrevLSN(lsn);
    log_manager_->Flush(false);
  }

  // Release all the locks.
  ReleaseLocks(txn);
  // Release the global transaction latch.
  global_txn_latch_.RUnlock();
}

void TransactionManager::BlockAllTransactions() { global_txn_latch_.WLock(); }

void TransactionManager::ResumeTransactions() { global_txn_latch_.WUnlock(); }

}  // namespace bustub
