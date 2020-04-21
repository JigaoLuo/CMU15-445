//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  assert(plan->IsRawInsert() ? child_executor_ == nullptr : child_executor_ != nullptr);
}

const Schema *InsertExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) {
  // 0. Init
  // 0.1. get table heap => tables
  const table_oid_t table_oid = plan_->GetTableOid();
  const TableMetadata *table_metadata = exec_ctx_->GetCatalog()->GetTable(table_oid);
  TableHeap *table_heap = table_metadata->table_.get();
  // 0.2. insert result as RID and bool
  RID rid;
  bool result = true;
  // 1. insert all values at one Next function call
  if (plan_->IsRawInsert()) {
    // raw insert: directly insert values from plan, where tuple have to built using table schema
    const Schema *table_schema = &table_metadata->schema_;
    assert(child_executor_ == nullptr);
    const std::vector<std::vector<Value>> &raw_vals = plan_->RawValues();
    for (const auto& raw_val : raw_vals) {
      result &= table_heap->InsertTuple(Tuple(raw_val, table_schema), &rid, exec_ctx_->GetTransaction());
      assert(rid.GetPageId() != INVALID_PAGE_ID);
    }
  } else {
    // non-raw insert: insert the values from child
    assert(child_executor_ != nullptr);
    Tuple tuple_from_child;
    while (child_executor_->Next(&tuple_from_child)) {
      result &= table_heap->InsertTuple(tuple_from_child, &rid, exec_ctx_->GetTransaction());
      assert(rid.GetPageId() != INVALID_PAGE_ID);
    }
  }
  return result;
}

}  // namespace bustub
