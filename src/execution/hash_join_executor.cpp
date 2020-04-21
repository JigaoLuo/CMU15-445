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
    // 1.0. new a tmp_tuple_page
    page_id_t tmp_tuple_page_id_;
    auto bpm_tmp_tuple_page = exec_ctx_->GetBufferPoolManager()->NewPage(&tmp_tuple_page_id_);
    bpm_tmp_tuple_page->WLatch();
    auto tmp_tuple_page = reinterpret_cast<TmpTuplePage *>(bpm_tmp_tuple_page->GetData());
    tmp_tuple_page->Init(tmp_tuple_page_id_, PAGE_SIZE);
    // 1.1. Get tuple from left side, insert into tmp_tuple_page
    TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
    while (left_executor_->Next(&t)) {
      if (!tmp_tuple_page->Insert(t, &tmp_tuple)) {
        // 1.1.1. tmp tuple page full, new a next one
        bpm_tmp_tuple_page->WUnlatch();
        exec_ctx_->GetBufferPoolManager()->UnpinPage(tmp_tuple_page_id_, true);
        bpm_tmp_tuple_page = exec_ctx_->GetBufferPoolManager()->NewPage(&tmp_tuple_page_id_);
        bpm_tmp_tuple_page->WLatch();
        tmp_tuple_page = reinterpret_cast<TmpTuplePage *>(bpm_tmp_tuple_page->GetData());
        tmp_tuple_page->Init(tmp_tuple_page_id_, PAGE_SIZE);
        [[maybe_unused]] bool tmp_insert_res = tmp_tuple_page->Insert(t, &tmp_tuple);
        assert(tmp_insert_res);
      }
      const hash_t hash_value = HashValues(&t, left_output_schema, plan_->GetLeftKeys());
      // 1.2. insert into hash table
      bool ht_insert_res = false;
      try {
        ht_insert_res = jht_.Insert(exec_ctx_->GetTransaction(), hash_value, tmp_tuple);
      } catch (hash_table_full_error) {
        // 1.2.1. resize hash table if necessary
        jht_.Resize(jht_.GetSize());
        ht_insert_res = jht_.Insert(exec_ctx_->GetTransaction(), hash_value, tmp_tuple);
      }
      assert(ht_insert_res);

    }
    jht_built_ = true;
    bpm_tmp_tuple_page->WUnlatch();
    exec_ctx_->GetBufferPoolManager()->UnpinPage(tmp_tuple_page_id_, true);
  }

  // 2.1. generate right tuple(s) until joined
  Tuple right_tuple_match;
  while (right_executor_->Next(&right_tuple_match)) {  // NOLINT
    // 2.2. probe hash table
    hash_t hash_value = HashJoinExecutor::HashValues(&right_tuple_match, right_output_schema, plan_->GetRightKeys());  // NOLINT
    std::vector<TmpTuple> left_tmp_tuple_matches;
    jht_.GetValue(exec_ctx_->GetTransaction(), hash_value, &left_tmp_tuple_matches);
    assert(!left_tmp_tuple_matches.empty());
    for (const auto& left_tmp_tuple_match : left_tmp_tuple_matches) {
      // 2.3. re-construct tuple from tmp_tuple_page
      Tuple left_tuple_match;
      auto bpm_tmp_tuple_page = exec_ctx_->GetBufferPoolManager()->FetchPage(left_tmp_tuple_match.GetPageId());
      bpm_tmp_tuple_page->RLatch();
      left_tuple_match.DeserializeFrom(bpm_tmp_tuple_page->GetData() + left_tmp_tuple_match.GetOffset());
      bpm_tmp_tuple_page->RUnlatch();
      exec_ctx_->GetBufferPoolManager()->UnpinPage(left_tmp_tuple_match.GetPageId(), false);
      // 2.4. evaluate join predicate using tuples
      if ((predicate != nullptr) ?
           predicate->EvaluateJoin(&left_tuple_match, left_output_schema,
                                   &right_tuple_match, right_output_schema).GetAs<bool>()
         : true) {
        // 2.5. ONLY one hash key matched and joined, build output tuple
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
