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

#ifndef STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_MANAGER_H_
#define STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_MANAGER_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "common/clock.h"
#include "frontend/collections/multiplexed_session_transaction_manager.h"
#include "frontend/common/labels.h"
#include "frontend/entities/database.h"
#include "frontend/entities/session.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Session manager manages the set of active sessions in the emulator.
class SessionManager {
 public:
  explicit SessionManager(Clock* clock) : clock_(clock) {}

  // Creates a session attached to the given database.
  absl::StatusOr<std::shared_ptr<Session>> CreateSession(
      const Labels& labels, bool multiplexed,
      std::shared_ptr<Database> database,
      MultiplexedSessionTransactionManager* mux_txn_manager);

  // Returns a session with the given URI.
  absl::StatusOr<std::shared_ptr<Session>> GetSession(
      const std::string& session_uri);

  // Deletes a session with the given URI.
  absl::Status DeleteSession(const std::string& session_uri,
                             bool delete_multiplex_sessions = false);

  // Lists sessions attached to the given database URI.
  absl::StatusOr<std::vector<std::shared_ptr<Session>>> ListSessions(
      const std::string& database_uri,
      bool include_multiplex_sessions = false) const;

  bool IsMultiplexedSession(const std::string& session_uri) const;

 private:
  // The session map is sharded by session URI so that concurrent requests on
  // unrelated sessions (the common case: many parallel databases, each with its
  // own sessions) do not serialize on a single process-global mutex. Every RPC
  // resolves its session through GetSession, so this lock sits on the hottest
  // path in the server; sharding it lets independent databases run in parallel.
  static constexpr size_t kNumShards = 64;

  struct Shard {
    mutable absl::Mutex mu;
    std::map<std::string, std::shared_ptr<Session>> session_map ABSL_GUARDED_BY(mu);
  };

  // Routes a session URI to its shard. Session URIs already embed a densely
  // incrementing id, so a cheap string hash spreads them evenly across shards.
  Shard& ShardFor(const std::string& session_uri) {
    return shards_[std::hash<std::string>{}(session_uri) & (kNumShards - 1)];
  }
  const Shard& ShardFor(const std::string& session_uri) const {
    return shards_[std::hash<std::string>{}(session_uri) & (kNumShards - 1)];
  }

  // System-wide clock.
  Clock* clock_;

  // Counter for session ids. Atomic so CreateSession never needs a global lock
  // just to mint the next id.
  std::atomic<int> next_session_id_ = 0;

  // Sharded session storage. kNumShards is a power of two so the shard index is
  // a mask, not a modulo.
  mutable std::array<Shard, kNumShards> shards_;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_MANAGER_H_
