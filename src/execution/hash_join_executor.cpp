//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>
#include <utility>

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left)),
      right_executor_(std::move(right)),
      jht_("ht", exec_ctx_->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_) {
  assert(left_executor_ != nullptr);
  assert(right_executor_ != nullptr);
}

void HashJoinExecutor::Init() {}

bool HashJoinExecutor::Next(Tuple *tuple) {
  // 0. Init
  // 0.1. INPUT SCHEMA: left right output_schema for predicate evaluation:
  //                    left right output_schema are INPUT for join
  const AbstractPlanNode *left_plan = plan_->GetLeftPlan();
  const Schema *left_output_schema = left_plan->OutputSchema();
  const AbstractPlanNode *right_plan = plan_->GetRightPlan();
  const Schema *right_output_schema = right_plan->OutputSchema();
  // 0.2. predicate
  const AbstractExpression *predicate = plan_->GetPredicate();
  // 0.3. OUTPUT SCHEMA: output stuff
  const Schema *final_output_schema = plan_->OutputSchema();
  const uint32_t final_output_schema_col_count = final_output_schema->GetColumnCount();
  const std::vector<Column> &final_output_cols = final_output_schema->GetColumns();

  // 1. if hash table not been built, then buld hash table using left tuples
  //    => pipeline breaker, blocked until left child empty
  Tuple t;
  if (!jht_built_) {  // NOLINT
    // 1.1. Get all tuple from left side
    std::vector<Tuple> tuples_from_left_child;
    while (left_executor_->Next(&t)) {
      tuples_from_left_child.emplace_back(t);
    }
    // 1.2. Build hash table
    for (const auto& left_tuple : tuples_from_left_child) {
      const hash_t hash_value = HashValues(&left_tuple, left_output_schema, plan_->GetLeftKeys());
      [[maybe_unused]] bool insert_res = jht_.Insert(exec_ctx_->GetTransaction(), hash_value, left_tuple);
      assert(insert_res);
    }
    jht_built_ = true;
  }

  // 2.1. generate right tuple(s) until joined
  Tuple right_tuple_match;
  while (right_executor_->Next(&right_tuple_match)) {  // NOLINT
    // 2.2. probe hash table
    hash_t hash_value = HashJoinExecutor::HashValues(&right_tuple_match, right_output_schema, plan_->GetRightKeys());  // NOLINT
    std::vector<Tuple> left_tuple_matches;
    jht_.GetValue(exec_ctx_->GetTransaction(), hash_value, &left_tuple_matches);
    assert(!left_tuple_matches.empty());
    for (const auto& left_tuple_match : left_tuple_matches) {
      if ((predicate != nullptr) ?
           predicate->EvaluateJoin(&left_tuple_match, left_output_schema,
                                   &right_tuple_match, right_output_schema).GetAs<bool>()
         : true) {
        // 2.3. ONLY one hash key matched and joined, build output tuple
        std::vector<Value> output_values;
        output_values.reserve(final_output_schema_col_count);
        for (size_t i = 0; i < final_output_schema_col_count; i++) {
          output_values.emplace_back
            (final_output_cols[i].GetExpr()->EvaluateJoin(&left_tuple_match, left_output_schema,
                                                          &right_tuple_match, right_output_schema));
        }
        *tuple = Tuple(output_values, final_output_schema);
        return true;
      }
    }
  }
  return false;
}
}  // namespace bustub
