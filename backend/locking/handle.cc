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

#include "backend/locking/handle.h"

#include <functional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/locking/manager.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

LockHandle::LockHandle(LockManager* manager, TransactionID tid,
                       const std::function<absl::Status()>& abort_fn,
                       TransactionPriority priority)
    : manager_(manager),
      tid_(tid),
      try_abort_transaction_fn_(abort_fn),
      priority_(priority) {}

LockHandle::~LockHandle() { manager_->UnlockAll(this); }

void LockHandle::EnqueueLock(const LockRequest& request) {
  manager_->EnqueueLock(this, request);
}

void LockHandle::UnlockAll() { manager_->UnlockAll(this); }

bool LockHandle::IsBlocked() { return manager_->IsBlocked(this); }

bool LockHandle::IsAborted() { return manager_->IsAborted(this); }

absl::Status LockHandle::Wait() { return manager_->Wait(this); }

absl::StatusOr<absl::Time> LockHandle::ReserveCommitTimestamp() {
  return manager_->ReserveCommitTimestamp(this);
}

absl::Status LockHandle::MarkCommitted() {
  return manager_->MarkCommitted(this);
}

void LockHandle::WaitForSafeRead(absl::Time read_time) {
  manager_->WaitForSafeRead(read_time);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
