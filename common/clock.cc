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

#include "common/clock.h"

#include <algorithm>
#include <cstdint>

#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

// Returns the current time at microsecond granularity, since the Unix epoch.
int64_t NowMicros() { return absl::ToUnixMicros(absl::Now()); }

}  // namespace

Clock::Clock() : last_dispensed_micros_(NowMicros()) {}

absl::Time Clock::Now() {
  const int64_t now = NowMicros();
  int64_t prev = last_dispensed_micros_.load(std::memory_order_relaxed);
  int64_t next;
  do {
    // Strictly monotonic: never hand out a value already dispensed, and track
    // the wall clock when it has advanced past the last value.
    next = std::max(now, prev + 1);
  } while (!last_dispensed_micros_.compare_exchange_weak(
      prev, next, std::memory_order_relaxed));
  return absl::FromUnixMicros(next);
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
