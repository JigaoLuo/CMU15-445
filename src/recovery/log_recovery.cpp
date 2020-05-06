//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
  // 0. Header check
  auto *casted_log_record = const_cast<LogRecord*>(reinterpret_cast<const LogRecord*>(data));
  const int32_t size = casted_log_record->size_;
  const lsn_t lsn = casted_log_record->lsn_;
  const txn_id_t txn_id = casted_log_record->txn_id_;
  const LogRecordType log_record_type = casted_log_record->log_record_type_;
  if (size < 0 || lsn == INVALID_LSN || txn_id == INVALID_TXN_ID || log_record_type == LogRecordType::INVALID) {
    return false;
  }

  // 1. Construct Header
  std::memcpy(reinterpret_cast<uint8_t*>(log_record), data, LogRecord::HEADER_SIZE);

  // 2. Construct the rest
  size_t pos = LogRecord::HEADER_SIZE;
  if (log_record_type == LogRecordType::BEGIN ||
      log_record_type == LogRecordType::COMMIT ||
      log_record_type == LogRecordType::ABORT) {
    // BEGIN, COMMIT, ABORT are Head Only => nothing to do
  } else if (log_record_type == LogRecordType::INSERT) {
    std::memcpy(&log_record->insert_rid_, data + pos, sizeof(RID));
    pos += sizeof(RID);
    log_record->insert_tuple_.DeserializeFrom(data + pos);
  } else if (log_record_type == LogRecordType::APPLYDELETE ||
             log_record_type == LogRecordType::MARKDELETE ||
             log_record_type == LogRecordType::ROLLBACKDELETE) {
    std::memcpy(&log_record->delete_rid_, data + pos, sizeof(RID));
    pos += sizeof(RID);
    log_record->delete_tuple_.DeserializeFrom(data + pos);
  } else if (log_record_type == LogRecordType::UPDATE) {
    std::memcpy(&log_record->update_rid_, data + pos, sizeof(RID));
    pos += sizeof(RID);
    log_record->old_tuple_.DeserializeFrom(data + pos);
    pos += sizeof(int32_t) + log_record->old_tuple_.GetLength();
    log_record->new_tuple_.DeserializeFrom(data + pos);
  } else if (log_record_type == LogRecordType::NEWPAGE) {
    std::memcpy(&log_record->prev_page_id_, data + pos, sizeof(page_id_t));
    pos += sizeof(page_id_t);
    std::memcpy(&log_record->page_id_, data + pos, sizeof(page_id_t));
  } else {
    return false;
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */

void LogRecovery::Redo() {
  assert(!enable_logging);
  while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
    size_t pos = 0;
    LogRecord log_record;
    while (DeserializeLogRecord(log_buffer_ + pos, &log_record)) {
      const LogRecordType log_record_type = log_record.log_record_type_;
      // REDO when page lsn < log record lsn
      // page lsn :=  the last (recent) lsn on the page
      const lsn_t lsn = log_record.lsn_;

      // Update active_txn_ := add new txn
      const txn_id_t txn_id = log_record.txn_id_;
      const auto it = active_txn_.find(txn_id);
      if (it == active_txn_.end()) {
        active_txn_.emplace(txn_id, lsn);
      } else {
        it->second = lsn;
      }

      // Update lsn_mapping_
      lsn_mapping_.emplace(lsn, offset_ + pos);
      pos += log_record.GetSize();

      if (log_record_type == LogRecordType::BEGIN) {
        // Do nothing
        assert(log_record.prev_lsn_ == INVALID_LSN);
      } else if (log_record_type == LogRecordType::COMMIT || log_record_type == LogRecordType::ABORT) {
        // Update active_txn_ := remove finished txn
        active_txn_.erase(it);
      } else if (log_record_type == LogRecordType::NEWPAGE) {
        const page_id_t prev_page_id = log_record.prev_page_id_;
        const page_id_t page_id = log_record.page_id_;
        assert(page_id != INVALID_PAGE_ID);
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        if (page->GetLSN() < lsn) {
          // Page LSN < Log Record LSN := Needs Redo
          page->WLatch();
          page->Init(page_id, PAGE_SIZE, prev_page_id, nullptr, nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page_id, true);

          // Fix prev page, if page is not the first page (having INVALID_PAGE_ID as prev_page)
          if (prev_page_id != INVALID_PAGE_ID) {
            auto prev_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(prev_page_id));
            assert(prev_page != nullptr);
            if (prev_page->GetNextPageId() != page_id) {
              prev_page->SetNextPageId(page_id);
              buffer_pool_manager_->UnpinPage(prev_page_id, false);
            } else {
              buffer_pool_manager_->UnpinPage(prev_page_id, true);
            }
          }
        } else {
          // Page LSN >= Log Record LSN := No Need For Redo
          buffer_pool_manager_->UnpinPage(page_id, false);
        }
      } else if (log_record_type == LogRecordType::INSERT) {
        auto &rid = log_record.insert_rid_;
        const auto page_id = rid.GetPageId();
        assert(page_id != INVALID_PAGE_ID);
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        if (page->GetLSN() < lsn) {
          // Page LSN < Log Record LSN := Needs Redo
          page->WLatch();
          page->InsertTuple(log_record.insert_tuple_, &rid, nullptr, nullptr, nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page_id, true);
        } else {
          // Page LSN >= Log Record LSN := No Need For Redo
          buffer_pool_manager_->UnpinPage(page_id, false);
        }
      } else if (log_record_type == LogRecordType::UPDATE) {
        auto &rid = log_record.update_rid_;
        const auto page_id = rid.GetPageId();
        assert(page_id != INVALID_PAGE_ID);
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        if (page->GetLSN() < lsn) {
          // Page LSN < Log Record LSN := Needs Redo
          page->WLatch();
          page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, rid, nullptr, nullptr, nullptr);
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page_id, true);
        } else {
          // Page LSN >= Log Record LSN := No Need For Redo
          buffer_pool_manager_->UnpinPage(page_id, false);
        }
      } else if (log_record_type == LogRecordType::MARKDELETE ||
                 log_record_type == LogRecordType::APPLYDELETE ||
                 log_record_type == LogRecordType::ROLLBACKDELETE) {
        auto &rid = log_record.delete_rid_;
        const auto page_id = rid.GetPageId();
        assert(page_id != INVALID_PAGE_ID);
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        if (page->GetLSN() < lsn) {
          // Page LSN < Log Record LSN := Needs Redo
          page->WLatch();
          if (log_record_type == LogRecordType::MARKDELETE) {
            page->MarkDelete(rid, nullptr, nullptr, nullptr);
          } else if (log_record_type == LogRecordType::APPLYDELETE) {
            page->ApplyDelete(rid, nullptr, nullptr);
          } else if (log_record_type == LogRecordType::ROLLBACKDELETE) {
            page->RollbackDelete(rid, nullptr, nullptr);
          }
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page_id, true);
        } else {
          // Page LSN >= Log Record LSN := No Need For Redo
          buffer_pool_manager_->UnpinPage(page_id, false);
        }
      } else {
        // Should not happen
        assert(false);
      }
    }
    // next disk read start from the last position we fail to deserialize a log record
    offset_ += pos;
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  assert(!enable_logging);
  for (const auto& active_txn : active_txn_) {
    lsn_t lsn = active_txn.second;
    while (lsn != INVALID_LSN) {
      // read the log
      const auto it = lsn_mapping_.find(lsn);
      assert(it != lsn_mapping_.end());
      auto offset = it->second;
      // assume no log record is larger than a page
      disk_manager_->ReadLog(log_buffer_, PAGE_SIZE, offset);
      LogRecord log_record;
      [[maybe_unusued]] const bool deserialization_res = DeserializeLogRecord(log_buffer_, &log_record);
      assert(deserialization_res);
      assert(lsn == log_record.lsn_);

      const LogRecordType log_record_type = log_record.log_record_type_;
      if (log_record_type == LogRecordType::BEGIN) {
        assert(log_record.prev_lsn_ == INVALID_LSN);
        // Do nothing
      } else if (log_record_type == LogRecordType::COMMIT || log_record_type == LogRecordType::ABORT) {
        // Should not happen
        assert(false);
      } else if (log_record_type == LogRecordType::NEWPAGE) {
        int is = 3;
        is++;
      } else if (log_record_type == LogRecordType::INSERT) {
        // Insert <-> ApplyDelete
        const RID &rid = log_record.insert_rid_;
        const page_id_t page_id = rid.GetPageId();
        assert(page_id != INVALID_PAGE_ID);
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        page->WLatch();
        page->ApplyDelete(rid, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, true);
      } else if (log_record_type == LogRecordType::UPDATE) {
        // Update <-> Update
        auto &rid = log_record.update_rid_;
        const page_id_t page_id = rid.GetPageId();
        assert(page_id != INVALID_PAGE_ID);
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        page->WLatch();
        Tuple new_tuple_rep;
        // We re-update the tuple := remove new tuple, insert old tuple
        page->UpdateTuple(log_record.old_tuple_, &new_tuple_rep, rid, nullptr, nullptr, nullptr);
        assert(new_tuple_rep.GetLength() == log_record.new_tuple_.GetLength() &&
               memcmp(new_tuple_rep.GetData(), log_record.new_tuple_.GetData(), new_tuple_rep.GetLength()) == 0);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
      } else if (log_record_type == LogRecordType::MARKDELETE ||
                 log_record_type == LogRecordType::APPLYDELETE ||
                 log_record_type == LogRecordType::ROLLBACKDELETE) {
        auto &rid = log_record.delete_rid_;
        const auto page_id = rid.GetPageId();
        assert(page_id != INVALID_PAGE_ID);
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
        assert(page != nullptr);
        page->WLatch();
        if (log_record_type == LogRecordType::MARKDELETE) {
          // MARKDELETE <-> ROLLBACKDELETE
          page->RollbackDelete(rid, nullptr, nullptr);
        } else if (log_record_type == LogRecordType::APPLYDELETE) {
          // APPLYDELETE <-> INSERT
          page->InsertTuple(log_record.delete_tuple_, &rid, nullptr, nullptr, nullptr);
        } else if (log_record_type == LogRecordType::ROLLBACKDELETE) {
          //  ROLLBACKDELETE <-> MARKDELETE
          page->MarkDelete(rid, nullptr, nullptr, nullptr);
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, true);
      }
      lsn = log_record.GetPrevLSN();
    }
  }
  active_txn_.clear();
  lsn_mapping_.clear();
}

}  // namespace bustub
