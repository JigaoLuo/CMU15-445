//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include <include/type/value_factory.h>

#include <vector>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
  : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_iterator(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())) {}  // NOLINT

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple) {
  // 0. Init
  // 0.1. INPUT SCHEMA: table_schema for predicate evaluation:
  //        must get column type for original table schema, since predicate can have no lint with output
  const table_oid_t table_oid = plan_->GetTableOid();
  const TableMetadata *table_metadata = exec_ctx_->GetCatalog()->GetTable(table_oid);
  const Schema *table_schema = &table_metadata->schema_;
  // 0.2. OUTPUT SCHEMA: output stuff
  const Schema *output_schema = plan_->OutputSchema();
  const uint32_t output_schema_col_count = output_schema->GetColumnCount();
  const std::vector<Column> &output_cols = output_schema->GetColumns();
  // 0.3. predicate
  const AbstractExpression *predicate = plan_->GetPredicate();
  // 0.4. get table heap => tables
  TableHeap *table_heap = table_metadata->table_.get();

  // 1. iterator all tuple
  //    original in table schema (not output tuple, not output schema)
  //      must get original tuple from table schema, since predicate can have no lint with output
  Tuple original_tuple;
  while (table_iterator != table_heap->End()) {
    original_tuple = *table_iterator++;
    if ((predicate != nullptr) ? predicate->Evaluate(&original_tuple, table_schema).GetAs<bool>() : true) {
      // 1.1 original tuple qualified, build output tuple
      std::vector<Value> output_values;
      output_values.reserve(output_schema_col_count);
      for (size_t i = 0; i < output_schema_col_count; i++) {
        output_values.emplace_back(output_cols[i].GetExpr()->Evaluate(&original_tuple, table_schema));
      }
      assert(output_values.size() == output_schema_col_count);
      *tuple = Tuple(output_values, output_schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
