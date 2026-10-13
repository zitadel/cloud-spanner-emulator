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

#include <atomic>
#include <map>
#include <memory>
#include <thread>  // NOLINT
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "common/config.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using ::googlesql_base::testing::StatusIs;

// Builds a single-column key from an integer, so tests can carve out disjoint
// and overlapping point/range locks.
Key IntKey(int64_t v) { return Key({googlesql::values::Int64(v)}); }

class LockManagerTest : public testing::Test {
 public:
  void SetUp() override {
    saved_probability_ = config::abort_current_transaction_probability();
  }
  void TearDown() override {
    config::set_abort_current_transaction_probability(saved_probability_);
  }

  Clock* clock() { return &clock_; }
  LockManager* manager() { return &manager_; }

  LockRequest ExclusiveAll(const TableID& table = "table") {
    return LockRequest(LockMode::kExclusive, table, KeyRange::All(), {});
  }
  LockRequest SharedAll(const TableID& table = "table") {
    return LockRequest(LockMode::kShared, table, KeyRange::All(), {});
  }
  LockRequest ExclusivePoint(int64_t v, const TableID& table = "table") {
    return LockRequest(LockMode::kExclusive, table, KeyRange::Point(IntKey(v)),
                       {});
  }

  // A handle that cannot be displaced (no abort callback).
  std::unique_ptr<LockHandle> Handle(int64_t tid, int64_t priority = 1) {
    return manager()->CreateHandle(TransactionID(tid), /*abort_fn=*/nullptr,
                                   TransactionPriority(priority));
  }
  // A handle whose owning transaction can always be aborted on demand, so a
  // conflicting requester is able to displace it.
  std::unique_ptr<LockHandle> DisplaceableHandle(int64_t tid,
                                                 int64_t priority = 1) {
    return manager()->CreateHandle(
        TransactionID(tid), []() -> absl::Status { return absl::OkStatus(); },
        TransactionPriority(priority));
  }

 private:
  Clock clock_;
  LockManager manager_ = LockManager(&clock_);
  int saved_probability_ = 0;
};

TEST_F(LockManagerTest, SingleTransactionAcquiresLock) {
  auto lh = Handle(/*tid=*/1);
  lh->EnqueueLock(ExclusiveAll());
  EXPECT_FALSE(lh->IsBlocked());
  GOOGLESQL_EXPECT_OK(lh->Wait());
}

TEST_F(LockManagerTest, SameRangeAccessConflicts) {
  // The emulator does not model shared-read concurrency: a second access to an
  // overlapping range conflicts even when both are reads. With displacement
  // off, the second one aborts and retries.
  config::set_abort_current_transaction_probability(0);
  auto lh1 = Handle(/*tid=*/1);
  auto lh2 = Handle(/*tid=*/2);

  lh1->EnqueueLock(SharedAll());
  GOOGLESQL_EXPECT_OK(lh1->Wait());

  lh2->EnqueueLock(SharedAll());
  EXPECT_TRUE(lh2->IsAborted());
  EXPECT_THAT(lh2->Wait(), StatusIs(absl::StatusCode::kAborted));
}

TEST_F(LockManagerTest, DisjointExclusiveRangesCoexist) {
  auto lh1 = Handle(/*tid=*/1);
  auto lh2 = Handle(/*tid=*/2);

  // Two exclusive locks on non-overlapping points run concurrently.
  lh1->EnqueueLock(ExclusivePoint(1));
  GOOGLESQL_EXPECT_OK(lh1->Wait());

  lh2->EnqueueLock(ExclusivePoint(2));
  EXPECT_FALSE(lh2->IsBlocked());
  EXPECT_FALSE(lh2->IsAborted());
  GOOGLESQL_EXPECT_OK(lh2->Wait());
}

TEST_F(LockManagerTest, DifferentTablesDoNotConflict) {
  auto lh1 = Handle(/*tid=*/1);
  auto lh2 = Handle(/*tid=*/2);

  lh1->EnqueueLock(ExclusiveAll("table_a"));
  GOOGLESQL_EXPECT_OK(lh1->Wait());

  lh2->EnqueueLock(ExclusiveAll("table_b"));
  EXPECT_FALSE(lh2->IsAborted());
  GOOGLESQL_EXPECT_OK(lh2->Wait());
}

TEST_F(LockManagerTest, ConflictAbortsRequesterWhenDisplacementDisabled) {
  // With displacement turned off, a conflicting requester always backs off and
  // the holder is untouched.
  config::set_abort_current_transaction_probability(0);
  auto holder = Handle(/*tid=*/1);
  auto requester = Handle(/*tid=*/2);

  holder->EnqueueLock(ExclusiveAll());
  GOOGLESQL_EXPECT_OK(holder->Wait());

  requester->EnqueueLock(ExclusiveAll());
  EXPECT_FALSE(requester->IsBlocked());
  EXPECT_TRUE(requester->IsAborted());
  EXPECT_THAT(requester->Wait(), StatusIs(absl::StatusCode::kAborted));
  EXPECT_FALSE(holder->IsAborted());
}

TEST_F(LockManagerTest, RequesterDisplacesHolderWhenConfigured) {
  // With displacement always on and a displaceable holder, a conflicting
  // requester takes over and the holder is aborted — never a block.
  config::set_abort_current_transaction_probability(100);
  auto holder = DisplaceableHandle(/*tid=*/1);
  auto requester = Handle(/*tid=*/2);

  holder->EnqueueLock(ExclusiveAll());
  GOOGLESQL_EXPECT_OK(holder->Wait());

  requester->EnqueueLock(ExclusiveAll());
  EXPECT_TRUE(holder->IsAborted());
  EXPECT_FALSE(requester->IsBlocked());
  GOOGLESQL_EXPECT_OK(requester->Wait());
}

TEST_F(LockManagerTest, RequesterSucceedsAfterHolderReleases) {
  config::set_abort_current_transaction_probability(0);
  auto holder = Handle(/*tid=*/1);
  auto requester = Handle(/*tid=*/2);

  holder->EnqueueLock(ExclusiveAll());
  GOOGLESQL_EXPECT_OK(holder->Wait());

  requester->EnqueueLock(ExclusiveAll());
  EXPECT_THAT(requester->Wait(), StatusIs(absl::StatusCode::kAborted));

  // Once the holder releases and the requester clears its abort (as a real
  // transaction does on retry via UnlockAll), the retry acquires the lock.
  holder->UnlockAll();
  requester->UnlockAll();
  requester->EnqueueLock(ExclusiveAll());
  GOOGLESQL_EXPECT_OK(requester->Wait());
}

TEST_F(LockManagerTest, WholeDatabaseLockDoesNotDisplaceLiveTransaction) {
  // Even with displacement always on and a displaceable data holder, a schema
  // change (whole-database lock: empty table id + KeyRange::All()) never
  // displaces a live transaction — it aborts itself instead, preserving the
  // emulator's contract that schema changes do not run concurrently with
  // read-write transactions.
  config::set_abort_current_transaction_probability(100);
  auto data = DisplaceableHandle(/*tid=*/2);
  auto schema = Handle(/*tid=*/1);

  data->EnqueueLock(ExclusivePoint(5, "some_table"));
  GOOGLESQL_EXPECT_OK(data->Wait());

  schema->EnqueueLock(
      LockRequest(LockMode::kExclusive, /*table_id=*/"", KeyRange::All(), {}));
  EXPECT_FALSE(data->IsAborted());
  EXPECT_THAT(schema->Wait(), StatusIs(absl::StatusCode::kAborted));

  // Once the transaction releases, the whole-database lock is available.
  data->UnlockAll();
  schema->UnlockAll();
  schema->EnqueueLock(
      LockRequest(LockMode::kExclusive, /*table_id=*/"", KeyRange::All(), {}));
  GOOGLESQL_EXPECT_OK(schema->Wait());
}

TEST_F(LockManagerTest, CommitTimestampsAreStrictlyIncreasing) {
  auto lh1 = Handle(/*tid=*/1);
  auto lh2 = Handle(/*tid=*/2);

  auto t1 = lh1->ReserveCommitTimestamp();
  GOOGLESQL_ASSERT_OK(t1);
  auto t2 = lh2->ReserveCommitTimestamp();
  GOOGLESQL_ASSERT_OK(t2);
  EXPECT_LT(*t1, *t2);

  GOOGLESQL_EXPECT_OK(lh1->MarkCommitted());
  GOOGLESQL_EXPECT_OK(lh2->MarkCommitted());
  EXPECT_EQ(manager()->LastCommitTimestamp(), *t2);
}

TEST_F(LockManagerTest, DisjointTransactionsRunInParallel) {
  // Each thread transactionally increments its own dedicated key. Because the
  // keys are disjoint, the transactions take non-conflicting exclusive locks
  // and make progress concurrently (no aborts, no serialization). tsan is
  // expected to catch any synchronization bug.
  const int n = 16;
  const int k = 25;
  std::vector<std::atomic<int>> counters(n);
  std::vector<std::thread> threads;
  std::atomic<int> id_counter(0);

  for (int i = 0; i < n; ++i) {
    threads.emplace_back(
        [&](int key) {
          for (int j = 0; j < k; ++j) {
            auto lh = manager()->CreateHandle(
                TransactionID(++id_counter), /*abort_fn=*/nullptr,
                TransactionPriority(1));
            lh->EnqueueLock(LockRequest(LockMode::kExclusive, "table",
                                        KeyRange::Point(IntKey(key)), {}));
            GOOGLESQL_ASSERT_OK(lh->Wait());
            counters[key].fetch_add(1);
            lh->UnlockAll();
          }
        },
        i);
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  for (int i = 0; i < n; ++i) {
    EXPECT_EQ(counters[i].load(), k);
  }
}

TEST_F(LockManagerTest, ContendedTransactionsSerializeAndConverge) {
  // All threads contend on the same key. With displacement off, a conflicting
  // requester aborts and retries while the holder keeps its lock until it
  // releases — every increment must still land without deadlock or lost
  // updates.
  config::set_abort_current_transaction_probability(0);
  std::map<absl::Time, int> value{{absl::InfinitePast(), 0}};
  auto GetValue = [&value] { return value.rbegin()->second; };

  const int n = 16;
  const int k = 25;
  std::vector<std::thread> threads;
  std::atomic<int> id_counter(0);
  absl::Mutex store_mu;

  for (int i = 0; i < n; ++i) {
    threads.emplace_back([&] {
      for (int j = 0; j < k; ++j) {
        while (true) {
          auto lh = manager()->CreateHandle(TransactionID(++id_counter),
                                            /*abort_fn=*/nullptr,
                                            TransactionPriority(1));
          lh->EnqueueLock(
              LockRequest(LockMode::kExclusive, "table", KeyRange::All(), {}));
          absl::Status status = lh->Wait();
          if (status.code() == absl::StatusCode::kAborted) {
            lh->UnlockAll();  // Release and retry, mirroring Reset().
            continue;
          }
          GOOGLESQL_ASSERT_OK(status);
          {
            absl::MutexLock l(&store_mu);
            int next = GetValue() + 1;
            value[clock()->Now()] = next;
          }
          lh->UnlockAll();
          break;
        }
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(n * k, GetValue());
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
