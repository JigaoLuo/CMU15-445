//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>

#include "recovery/log_manager.h"
#include "common/logger.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
  // 1. Set enable_logging := true
  if (enable_logging) {
    return;
  }
  enable_logging = true;
  // 2. Start a separate thread to execute flush to disk operation periodically
  flush_thread_ = new std::thread([&]{
    while (enable_logging) {
      // The flush can be triggered when timeout or the log buffer is full
      // or buffer pool manager wants to force flush
      // (it only happens when the flushed page has a larger LSN than persistent LSN)
      std::unique_lock<std::mutex> FlushThreadLatched(latch_);
      cv_.wait_for(FlushThreadLatched, log_timeout, [&]{return needs_flush.load();});
      assert(flush_buffer_write_offset == 0);
      if (log_buffer_write_offset > 0) {
        // really needs to flush
        // You should swap buffers under any of the following three situations.
        // (1) When the log buffer is full
        // (2) when log_timeout seconds have passed
        // (3) When the buffer pool is going to evict a dirty page from the LRU replacer
        std::swap(log_buffer_, flush_buffer_);
        std::swap(log_buffer_write_offset, flush_buffer_write_offset);
        assert(log_buffer_write_offset == 0);
        LOG_INFO("LogManager::RunFlushThread := Flushing Log to Disk.");  // NOLINT
        disk_manager_->WriteLog(flush_buffer_, flush_buffer_write_offset);
        flush_buffer_write_offset = 0;
        SetPersistentLSN(lastLsn_);
      }
      needs_flush = false;
      append_cv_.notify_all();
    }
  });
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {

  assert(enable_logging);
  enable_logging = false;
  Flush(true);
  flush_thread_->join();
  assert(log_buffer_write_offset == 0);
  assert(flush_buffer_write_offset == 0);
  delete flush_thread_;
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  std::unique_lock<std::mutex> AppendLogRecordatched(latch_);
  // 1. log buffer would be full => one of the flush condition is statisfied
  //    => flush log buffer out
  if (log_buffer_write_offset + log_record->size_ > LOG_BUFFER_SIZE) {
    // wake up flush_thread_ to flush
    LOG_INFO("LogManager::AppendLogRecord := Log Buffer Full, Triggering a Flushing Log to Disk.");  // NOLINT
    needs_flush = true;
    cv_.notify_one();
    append_cv_.wait(AppendLogRecordatched,
                    [&] { return log_buffer_write_offset + log_record->size_ <= LOG_BUFFER_SIZE; });
  }

  // Code example given from Project + log_record.h
  // 2. serialize the must have fields(20 bytes in total)
  //    20 Bytes := LogRecord::HEADER_SIZE
  //    + call provided serialize function for tuple class
  log_record->lsn_ = next_lsn_++;
  std::memcpy(log_buffer_ + log_buffer_write_offset, reinterpret_cast<uint8_t*>(log_record), LogRecord::HEADER_SIZE);
  int pos = log_buffer_write_offset + LogRecord::HEADER_SIZE;

  const LogRecordType log_record_type = log_record->log_record_type_;
  if (log_record_type == LogRecordType::BEGIN ||
      log_record_type == LogRecordType::COMMIT ||
      log_record_type == LogRecordType::ABORT) {
    // BEGIN, COMMIT, ABORT are Head Only => nothing to do
  } else if (log_record_type == LogRecordType::INSERT) {
     std::memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
     pos += sizeof(RID);
     log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record_type == LogRecordType::APPLYDELETE ||
             log_record_type == LogRecordType::MARKDELETE ||
             log_record_type == LogRecordType::ROLLBACKDELETE) {
    std::memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record_type == LogRecordType::UPDATE) {
    std::memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += 4 /* sizeof(int32_t) */+ static_cast<int>(log_record->old_tuple_.GetLength());
    log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record_type == LogRecordType::NEWPAGE) {
    std::memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
    pos += sizeof(page_id_t);
    std::memcpy(log_buffer_ + pos, &log_record->page_id_, sizeof(page_id_t));
  }
  log_buffer_write_offset += log_record->size_;
  return lastLsn_ = log_record->lsn_;
}

void LogManager::Flush(bool if_force) {
  std::unique_lock<std::mutex> FlushLatched(latch_);
  if (if_force) {
    // wake up flush_thread_ to flush
    needs_flush = true;
    cv_.notify_one();
    if (enable_logging) {
      append_cv_.wait(FlushLatched,
                      [&] { return !needs_flush.load(); });
    }
  } else {
    append_cv_.wait(FlushLatched);
  }
}

}  // namespace bustub
