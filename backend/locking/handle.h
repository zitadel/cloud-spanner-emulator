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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_HANDLE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_HANDLE_H_

#include <functional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/locking/request.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Forward declaration of the LockManager to avoid a circular reference.
class LockManager;

// LockHandle encapsulates a transaction's interface to the lock manager.
//
// A transaction first creates a lock handle via LockManager::CreateHandle().
// All subsequent communication between the transaction and the LockManager
// happens via the LockHandle. This includes incremental acquisition of read
// write locks, waiting on those locks, and unlocking those locks.
//
// EnqueueLock() records a lock request. Wait() blocks until every request made
// since the last Wait()/UnlockAll() has been granted, or the transaction has
// been wounded (in which case Wait() returns ABORTED). All mutable lock state
// lives in the LockManager and is guarded by its mutex; the handle itself is an
// immutable addressable identity for one transaction.
//
// Usage (happy path, error handling skipped):
//    // Get a handle.
//    auto handle = lock_manager->CreateHandle(txn_id, abort_fn, txn_priority);
//
//    // Request some locks.
//    handle->EnqueueLock(...)
//    handle->EnqueueLock(...)
//
//    // Wait for those locks to be granted (blocks under contention).
//    handle->Wait()
//
//    // Do work based on those locks.
//    ...
//
//    // Unlock all locks held by this transaction.
//    handle->UnlockAll();
class LockHandle {
 public:
  // Returns the ID of the transaction which owns this handle.
  TransactionID tid() { return tid_; }

  // Returns the priority of the transaction which owns this handle.
  TransactionPriority priority() { return priority_; }

  // Enqueues a lock request for this transaction. Lock requests from aborted
  // handles are ignored by the lock manager.
  void EnqueueLock(const LockRequest& request);

  // Unlocks all locks held by this transaction and clears any aborted state, so
  // the handle can be reused if the transaction retries. Frees any waiters
  // blocked on these locks.
  void UnlockAll();

  // Returns true if this handle has a request that has not yet been granted.
  bool IsBlocked();

  // Returns true if this handle has been wounded (aborted) by the lock manager
  // and not yet released via UnlockAll().
  bool IsAborted();

  // Blocks until all locks requested via this handle have been granted, or the
  // transaction has been wounded. Wounds return ABORTED, otherwise OK.
  absl::Status Wait();

  // Returns a timestamp which this transaction may use as a commit timestamp.
  // Returns ABORTED if the transaction has been wounded.
  absl::StatusOr<absl::Time> ReserveCommitTimestamp();

  // Notifies the LockManager that this transaction has committed.
  absl::Status MarkCommitted();

  // Waits for the intended read timestamp to be safe from any in-progress
  // commits.
  void WaitForSafeRead(absl::Time read_time);

 private:
  // Only the LockManager is allowed to create and destroy LockHandles, and it
  // reads try_abort_transaction_fn_ when wounding this transaction.
  friend class LockManager;
  friend std::unique_ptr<LockHandle>::deleter_type;
  LockHandle(LockManager* manager, TransactionID tid,
             const std::function<absl::Status()>& abort_fn,
             TransactionPriority priority);
  // Releases every lock, pending request, abort marker, and reserved commit
  // timestamp this handle holds in the manager, so a transaction destroyed
  // without an explicit UnlockAll (e.g. a wounded transaction discarded on
  // retry) never leaves a dangling handle pointer in the manager's tables.
  ~LockHandle();

  // The LockManager which this LockHandle interacts with.
  LockManager* const manager_;

  // The ID of the transaction which owns this lock handle.
  const TransactionID tid_;

  // A best-effort callback to abort the owning transaction when it is wounded.
  // Returns OK only if the transaction was successfully aborted. May be nullptr
  // when the transaction cannot be aborted this way (e.g. schema changes).
  const std::function<absl::Status()> try_abort_transaction_fn_;

  // The priority (age) of the transaction which owns this lock handle. Smaller
  // is older; older transactions win under wound-wait.
  const TransactionPriority priority_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_HANDLE_H_
