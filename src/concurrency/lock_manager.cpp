//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // TODO(student):
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // TODO(student):
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // TODO(student):
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // TODO(student):
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  assert(Detection());
  graph.emplace_back(t1, t2);
  waits_for_[t1].emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  assert(Detection());
  graph.erase(std::find(graph.begin(), graph.end(), std::make_pair(t1, t2)));
  const auto hs_it = waits_for_.find(t1);
  assert(hs_it != waits_for_.end());
  auto& vec = hs_it->second;
  vec.erase(std::find(vec.begin(), vec.end(), t2));
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  BUSTUB_ASSERT(Detection(), "Detection should be enabled!");
  for (auto it = graph.crbegin(); it != graph.crend(); ++it) {
    const auto hs_it = waits_for_.find(it->second);
    if (hs_it != waits_for_.end()) {
      auto& vec = hs_it->second;
      if (std::find(vec.begin(), vec.end(), it->first) != vec.end()) {
        *txn_id = it->first;
        return true;
      }
    }
  }
  return false;
}

std::vector<LockManager::GraphEdge> LockManager::GetEdgeList() {
  BUSTUB_ASSERT(Detection(), "Detection should be enabled!");
  return graph;
}

void LockManager::RunCycleDetection() {
  BUSTUB_ASSERT(Detection(), "Detection should be enabled!");
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      txn_id_t txn_id;
      if (HasCycle(&txn_id)) {
        // TODO(student):
      }
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
