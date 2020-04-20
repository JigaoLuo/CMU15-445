//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>
#include <utility>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {}

bool AggregationExecutor::Next(Tuple *tuple) {
  // 0. Init
  const std::vector<const AbstractExpression *> &group_bys = plan_->GetGroupBys();
  const std::vector<const AbstractExpression*> &aggregates = plan_->GetAggregates();
  const size_t aggregate_num = aggregates.size();
  // 0.1. INPUT SCHEMA: child output_schema is INPUT for aggregation
  const AbstractPlanNode *child_plan = plan_->GetChildPlan();
  const Schema *child_output_schema = child_plan->OutputSchema();
  // 0.2. OUTPUT SCHEMA: output stuff
  const Schema *output_schema = plan_->OutputSchema();

  // 1. do aggregation depending if having group by clause or not
  if (group_bys.empty()) {
    // 1.1. no group by => no hash table, calculate ONCE
    AggregateValue aggregate_value = aht_.GenerateInitialAggregateValue();
    Tuple tuple_from_child;
    bool looped = false;
    while (child_->Next(&tuple_from_child)) {
      looped = true;
      AggregateValue input;
      input.aggregates_.reserve(aggregate_num);
      for (const auto& aggregate : aggregates) {
        input.aggregates_.emplace_back(aggregate->Evaluate(&tuple_from_child, child_output_schema));
      }
      aht_.CombineAggregateValues(&aggregate_value, input);
    }
    if (!looped) {
      return false;
    }
    // return only single tuple back
    *tuple = Tuple(aggregate_value.aggregates_, output_schema);
    return true;
  }
  // 1.2. with group by => build hash table
  const AbstractExpression *having = plan_->GetHaving();
  const uint32_t output_schema_col_count = output_schema->GetColumnCount();
  const std::vector<Column> &output_cols = output_schema->GetColumns();
  // 1.2.1. build hash table
  Tuple tuple_from_child;
  while (!executed && child_->Next(&tuple_from_child)) {
    AggregateKey agg_key;
    agg_key.group_bys_.reserve(group_bys.size());
    for (const auto& group_by : group_bys) {
      agg_key.group_bys_.emplace_back(group_by->Evaluate(&tuple_from_child, child_output_schema));
    }
    AggregateValue agg_val;
    agg_val.aggregates_.reserve(aggregate_num);
    for (const auto& aggregate : aggregates) {
      agg_val.aggregates_.emplace_back(aggregate->Evaluate(&tuple_from_child, child_output_schema));
    }
    aht_.InsertCombine(agg_key, agg_val);
  }
  // 1.2.2. configure after hash table built
  if (!executed) {
    executed = true;
    aht_iterator_ = aht_.Begin();
  }
  // 1.2.3. iterator hash table
  while (aht_iterator_ != aht_.End()) {
    const std::vector<Value>& aggreage_key = aht_iterator_.Key().group_bys_;
    const std::vector<Value>& aggreage_values = aht_iterator_.Val().aggregates_;
    ++aht_iterator_;
    // 1.2.4. check having predicate
    if ((having != nullptr) ? having->EvaluateAggregate(aggreage_key, aggreage_values).GetAs<bool>() : true) {
      std::vector<Value> output_values;
      output_values.reserve(output_schema_col_count);
      for (size_t i = 0; i < output_schema_col_count; i++) {
        output_values.emplace_back(output_cols[i].GetExpr()->EvaluateAggregate(aggreage_key, aggreage_values));
      }
      *tuple = Tuple(output_values, output_schema);
      return true;
    }
  }
  return false;
  }
}  // namespace bustub
