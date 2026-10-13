//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/locking/manager.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key_range.h"
#include "common/config.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

std::unique_ptr<LockHandle> LockManager::CreateHandle(
    TransactionID tid, const std::function<absl::Status()>& abort_fn,
    TransactionPriority priority) {
  return absl::WrapUnique(new LockHandle(this, tid, abort_fn, priority));
}

bool LockManager::RangesOverlap(const KeyRange& a, const KeyRange& b) {
  // Normalize both to closed-open [start, limit) and check for intersection.
  // [as, al) and [bs, bl) overlap iff as < bl and bs < al.
  const KeyRange ca = a.ToClosedOpen();
  const KeyRange cb = b.ToClosedOpen();
  return ca.start_key() < cb.limit_key() && cb.start_key() < ca.limit_key();
}

bool LockManager::Conflicts(const LockRequest& request, const Grant& grant) {
  // Any two locks whose key ranges overlap on the same table conflict,
  // regardless of mode. The emulator does not model shared-read concurrency: a
  // row is accessed by one transaction at a time (the original whole-database
  // behavior), now scoped to the key range so transactions on disjoint data run
  // in parallel. Modeling shared locks would reintroduce the read-then-upgrade
  // deadlock, where many readers each try to upgrade a shared lock on the same
  // row to exclusive and none can proceed.
  //
  // An empty table id denotes a whole-database (schema change) lock and matches
  // every table.
  const bool table_matches = request.table_id().empty() ||
                             grant.table_id.empty() ||
                             request.table_id() == grant.table_id;
  if (!table_matches) {
    return false;
  }
  return RangesOverlap(request.key_range(), grant.key_range);
}

void LockManager::EnqueueLock(LockHandle* handle, const LockRequest& request) {
  absl::MutexLock lock(&mu_);
  // Lock requests from aborted handles are ignored.
  if (aborted_.contains(handle)) {
    return;
  }
  pending_.insert_or_assign(handle, request);
  // Resolve immediately — grant if compatible, abort the requester if not. A
  // subsequent Wait() just reports the outcome; it never blocks.
  TryGrant(handle);
}

bool LockManager::TryGrant(LockHandle* handle) {
  auto it = pending_.find(handle);
  if (it == pending_.end()) {
    return true;  // Nothing pending; vacuously granted.
  }
  const LockRequest request = it->second;

  // Collect the distinct conflicting holders (other transactions whose lock
  // overlaps this request with an incompatible mode).
  std::vector<LockHandle*> holders;
  bool schema_involved = request.table_id().empty();
  for (const Grant& grant : grants_) {
    if (grant.handle == handle) continue;
    if (!Conflicts(request, grant)) continue;
    if (grant.table_id.empty()) schema_involved = true;
    if (std::find(holders.begin(), holders.end(), grant.handle) ==
        holders.end()) {
      holders.push_back(grant.handle);
    }
  }

  if (holders.empty()) {
    // No conflict — grant the lock.
    grants_.push_back(Grant{handle, request.mode(), request.table_id(),
                            request.key_range()});
    pending_.erase(handle);
    return true;
  }

  // There is a conflict. A schema change never displaces a live transaction,
  // and a live transaction never displaces a schema change: the requester
  // aborts. Otherwise, with the configured probability, try to displace every
  // conflicting holder and take over; if any cannot be displaced, back off.
  if (!schema_involved) {
    absl::BitGen gen;
    if (absl::uniform_int_distribution<int>(1, 100)(gen) <=
        config::abort_current_transaction_probability()) {
      bool displaced_all = true;
      for (LockHandle* holder : holders) {
        if (aborted_.contains(holder)) continue;  // already displaced
        if (holder->try_abort_transaction_fn_ != nullptr &&
            holder->try_abort_transaction_fn_().ok()) {
          aborted_.insert_or_assign(holder, error::AbortCurrentTransaction(
                                                holder->tid(), handle->tid()));
        } else {
          displaced_all = false;
        }
      }
      if (displaced_all) {
        // Drop the displaced holders' locks and grant the requester. An aborted
        // transaction can no longer read, write, or commit, and releases the
        // rest of its state when it unwinds.
        grants_.erase(
            std::remove_if(grants_.begin(), grants_.end(),
                           [&holders](const Grant& g) {
                             return std::find(holders.begin(), holders.end(),
                                              g.handle) != holders.end();
                           }),
            grants_.end());
        grants_.push_back(Grant{handle, request.mode(), request.table_id(),
                                request.key_range()});
        pending_.erase(handle);
        return true;
      }
    }
  }

  // Abort the requester; it will retry.
  aborted_.insert_or_assign(
      handle,
      error::AbortConcurrentTransaction(handle->tid(), holders.front()->tid()));
  pending_.erase(handle);
  return false;
}

absl::Status LockManager::Wait(LockHandle* handle) {
  absl::MutexLock lock(&mu_);
  auto aborted = aborted_.find(handle);
  if (aborted != aborted_.end()) {
    return aborted->second;
  }
  if (TryGrant(handle)) {
    return absl::OkStatus();
  }
  // TryGrant aborted us on a conflict; return that status.
  return aborted_.at(handle);
}

bool LockManager::IsBlocked(LockHandle* handle) {
  absl::MutexLock lock(&mu_);
  // Lock acquisition never blocks: a request is either granted or aborted
  // synchronously, so a handle is never left pending.
  return false;
}

bool LockManager::IsAborted(LockHandle* handle) {
  absl::MutexLock lock(&mu_);
  return aborted_.contains(handle);
}

void LockManager::UnlockAll(LockHandle* handle) {
  absl::MutexLock lock(&mu_);
  grants_.erase(std::remove_if(grants_.begin(), grants_.end(),
                               [handle](const Grant& g) {
                                 return g.handle == handle;
                               }),
                grants_.end());
  pending_.erase(handle);
  // Clear any aborted marker so the same handle can be reused if the
  // transaction retries (retries reuse the handle and re-acquire locks).
  aborted_.erase(handle);
  // Drop any commit timestamp reserved but never committed (e.g. the
  // transaction is being torn down mid-commit), so a concurrent snapshot read
  // does not wait forever for a commit that will never land.
  auto reserved = reserved_ts_.find(handle);
  if (reserved != reserved_ts_.end()) {
    auto pending_commit = pending_commits_.find(reserved->second);
    if (pending_commit != pending_commits_.end()) {
      pending_commits_.erase(pending_commit);
    }
    reserved_ts_.erase(reserved);
  }
  cv_.SignalAll();
}

absl::StatusOr<absl::Time> LockManager::ReserveCommitTimestamp(
    LockHandle* handle) {
  absl::MutexLock lock(&mu_);
  // A wounded transaction must not be allowed to commit.
  auto aborted = aborted_.find(handle);
  if (aborted != aborted_.end()) {
    return aborted->second;
  }
  absl::Time ts = clock_->Now();
  if (ts <= last_reserved_timestamp_) {
    ts = last_reserved_timestamp_ + absl::Nanoseconds(1);
  }
  last_reserved_timestamp_ = ts;
  reserved_ts_.insert_or_assign(handle, ts);
  pending_commits_.insert(ts);
  return ts;
}

absl::Status LockManager::MarkCommitted(LockHandle* handle) {
  absl::MutexLock lock(&mu_);
  auto it = reserved_ts_.find(handle);
  if (it != reserved_ts_.end()) {
    const absl::Time ts = it->second;
    auto pending = pending_commits_.find(ts);
    if (pending != pending_commits_.end()) {
      pending_commits_.erase(pending);
    }
    last_commit_timestamp_ = std::max(last_commit_timestamp_, ts);
    reserved_ts_.erase(it);
  }
  cv_.SignalAll();
  return absl::OkStatus();
}

void LockManager::WaitForSafeRead(absl::Time read_time) {
  absl::MutexLock lock(&mu_);
  // Wait for the wall clock to reach the read timestamp (an always-false
  // condition turns AwaitWithDeadline into a plain sleep until the deadline).
  const bool never = false;
  mu_.AwaitWithDeadline(absl::Condition(&never), read_time);
  // Then wait until no in-flight commit carries a timestamp earlier than the
  // read, so the snapshot sees every commit that ordered before it.
  while (!pending_commits_.empty() && *pending_commits_.begin() < read_time) {
    cv_.Wait(&mu_);
  }
}

absl::Time LockManager::LastCommitTimestamp() {
  absl::MutexLock lock(&mu_);
  return last_commit_timestamp_;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
