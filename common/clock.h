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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CLOCK_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CLOCK_H_

#include <atomic>
#include <cstdint>

#include "absl/time/time.h"

namespace google {
namespace spanner {
namespace emulator {

// Clock implements a strictly monotonic clock.
//
// This class provides two properties that are useful for the emulator:
//
// - The values returned from `Now()` are strictly monotonic - two calls to
//   `Now()` will never return the same value, and values returned from `Now()`
//   are always increasing. This allows us to use values from `Now()` as
//   commit timestamps.
//
// - The values returned from `Now()` are truncated to microsecond resolution.
//   This is to conform with Cloud Spanner's commit timestamps which also
//   operate at microsecond resolution.
//
// This class is thread safe and lock-free: `Now()` dispenses strictly
// increasing microsecond timestamps with a single atomic compare-exchange, so
// transactions across every database can take commit timestamps concurrently
// without serializing on a global mutex.
class Clock {
 public:
  Clock();

  // Returns the current time.
  absl::Time Now();

 private:
  // The last microsecond value handed out by Now(), as microseconds since the
  // Unix epoch. Updated with a lock-free compare-exchange.
  std::atomic<int64_t> last_dispensed_micros_;
};

}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_CLOCK_H_
