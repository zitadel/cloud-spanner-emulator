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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_MANAGER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_MANAGER_H_

#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key_range.h"
#include "backend/locking/handle.h"
#include "backend/locking/request.h"
#include "common/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// LockManager implements range-based locking for a single database.
//
// This replaces the emulator's original whole-database lock (one transaction at
// a time) with a per-key-range lock: transactions run concurrently as long as
// the key ranges they touch do not overlap. Conflicts are evaluated at (table,
// key-range) granularity — any two locks on overlapping ranges of the same
// table conflict, regardless of read/write mode. The emulator deliberately does
// not model shared-read concurrency (see Conflicts()); a range is accessed by
// one transaction at a time, exactly as before, only now that exclusion is
// per-range instead of per-database. Because almost every table is keyed by a
// leading project id, transactions on different projects touch disjoint ranges
// and never contend.
//
// A schema change still takes a whole-database lock, modeled as a request with
// an empty table id and KeyRange::All(); an empty table id matches every table,
// so such a lock conflicts with everything.
//
// Conflicts are resolved exactly as the emulator always has — probabilistically
// and without ever blocking a thread — the only change being that two locks now
// conflict when their key ranges overlap, rather than treating every lock as a
// whole-database lock. On a conflict with one or more holders:
//
//   - with probability config::abort_current_transaction_probability(), the
//     requester tries to abort (displace) every conflicting holder and take
//     over; if it displaces them all, it is granted,
//   - otherwise (or if a holder could not be displaced), the requester aborts
//     itself and retries.
//
// Nothing ever waits on a lock, so no wait cycle — and hence no deadlock — can
// form, and the common single-threaded "abort and retry" callers never hang.
// Probabilistic displacement both avoids livelock under the read-then-upgrade
// pattern and ensures a transaction holding locks without committing cannot
// wedge others indefinitely — it is eventually displaced. Because only
// overlapping key ranges conflict, transactions on disjoint data (for example
// different primary-key prefixes) never contend and run fully in parallel.
//
// A schema change (whole-database lock) is the exception: on any conflict the
// schema requester aborts itself rather than displacing live transactions,
// preserving the emulator's contract that schema changes do not run
// concurrently with read-write transactions.
//
// Locks are held until the transaction commits or rolls back (strict 2PL),
// which — combined with writes being buffered in the transaction store until
// commit — makes reading the latest committed value under lock serializable
// without a separate MVCC snapshot for read-write transactions.
class LockManager {
 public:
  explicit LockManager(Clock* clock) : clock_(clock) {}

  // Returns a handle for a single transaction with the given id and priority.
  // Subsequent communication between the transaction and the lock manager
  // happens via the handle. See LockHandle methods for more details.
  std::unique_ptr<LockHandle> CreateHandle(
      TransactionID id, const std::function<absl::Status()>& abort_fn,
      TransactionPriority priority);

  // Returns the timestamp at which the last commit completed.
  absl::Time LastCommitTimestamp() ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // LockHandle forwards all of its requests to the LockManager.
  friend class LockHandle;

  void EnqueueLock(LockHandle* handle, const LockRequest& request)
      ABSL_LOCKS_EXCLUDED(mu_);
  void UnlockAll(LockHandle* handle) ABSL_LOCKS_EXCLUDED(mu_);
  bool IsBlocked(LockHandle* handle) ABSL_LOCKS_EXCLUDED(mu_);
  bool IsAborted(LockHandle* handle) ABSL_LOCKS_EXCLUDED(mu_);
  absl::Status Wait(LockHandle* handle) ABSL_LOCKS_EXCLUDED(mu_);
  absl::StatusOr<absl::Time> ReserveCommitTimestamp(LockHandle* handle)
      ABSL_LOCKS_EXCLUDED(mu_);
  absl::Status MarkCommitted(LockHandle* handle) ABSL_LOCKS_EXCLUDED(mu_);
  void WaitForSafeRead(absl::Time read_time) ABSL_LOCKS_EXCLUDED(mu_);

  // A single granted lock, held until the owning transaction releases it.
  struct Grant {
    LockHandle* handle;
    LockMode mode;
    TableID table_id;
    KeyRange key_range;
  };

  // Resolves the request currently pending for `handle`: grants it when it does
  // not conflict with any lock held by another transaction, otherwise aborts
  // `handle` (recording the abort status). Returns true if the handle has
  // nothing pending or its request was granted, false if it was aborted.
  bool TryGrant(LockHandle* handle) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // True if `request` conflicts with `grant` (incompatible modes on an
  // overlapping key range of a matching table).
  static bool Conflicts(const LockRequest& request, const Grant& grant);

  // True if the two half-open-normalized key ranges overlap.
  static bool RangesOverlap(const KeyRange& a, const KeyRange& b);

  Clock* const clock_;

  absl::Mutex mu_;

  // Signalled whenever locks are released, granted, or a handle is aborted, so
  // that waiters re-evaluate their pending request.
  absl::CondVar cv_ ABSL_GUARDED_BY(mu_);

  // All currently granted locks across all transactions.
  std::vector<Grant> grants_ ABSL_GUARDED_BY(mu_);

  // The outstanding request for each handle that has not yet been granted.
  absl::flat_hash_map<LockHandle*, LockRequest> pending_ ABSL_GUARDED_BY(mu_);

  // Non-OK for any handle that has been aborted (wounded) and not yet released.
  absl::flat_hash_map<LockHandle*, absl::Status> aborted_ ABSL_GUARDED_BY(mu_);

  // The commit timestamp reserved by each handle that is mid-commit.
  absl::flat_hash_map<LockHandle*, absl::Time> reserved_ts_ ABSL_GUARDED_BY(mu_);

  // Commit timestamps reserved but not yet marked committed. A snapshot read at
  // time t must wait until no in-flight commit carries a timestamp < t.
  std::multiset<absl::Time> pending_commits_ ABSL_GUARDED_BY(mu_);

  // Timestamp of the last completed commit.
  absl::Time last_commit_timestamp_ ABSL_GUARDED_BY(mu_) = absl::InfinitePast();

  // Largest commit timestamp handed out so far; keeps reserved timestamps
  // strictly increasing and unique even when the clock does not advance between
  // two concurrent commits.
  absl::Time last_reserved_timestamp_ ABSL_GUARDED_BY(mu_) = absl::InfinitePast();
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_MANAGER_H_
