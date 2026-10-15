#pragma once

#include <array>
#include <optional>
#include <string>

namespace fleet {

// The six states a vehicle task moves through. Values are the exact
// strings stored in Postgres, so a schema dump is self-describing without
// a lookup table.
enum class TaskState {
  kDispatch,
  kEnRoute,
  kOnTrip,
  kCharging,
  kCleaning,
  kOutOfService,
};

constexpr std::array<TaskState, 6> kAllStates = {
    TaskState::kDispatch, TaskState::kEnRoute,     TaskState::kOnTrip,
    TaskState::kCharging, TaskState::kCleaning, TaskState::kOutOfService,
};

std::string ToString(TaskState s);
std::optional<TaskState> FromString(const std::string& s);

// True if `to` is a legal next state directly from `from` in the task
// state machine: dispatch -> en_route -> on_trip -> {charging, cleaning,
// out_of_service}; charging/cleaning -> {dispatch, out_of_service};
// dispatch/en_route can also fail straight to out_of_service. Every
// transition the reconciler or the coordinator writes is checked against
// this, so an illegal transition is a rejected write, not a silent one.
bool IsLegalTransition(TaskState from, TaskState to);

// Safety rank used by the reconciler: higher is safer to *assume* when two
// sources disagree about a vehicle's state. out_of_service is safest to
// assume (worst case, a vehicle that is actually fine sits idle an extra
// cycle); on_trip is least safe to assume (worst case, a vehicle that is
// actually stopped is dispatched into traffic believing it is mid-trip).
// This is a total order, not the task state machine's own transition
// order: reconciliation does not have to go through a legal transition to
// reach the safe side, by design, since the whole point is responding to
// telemetry the state machine did not expect.
int SafetyRank(TaskState s);

// Resolves a disagreement between the recorded state and a freshly
// reported telemetry state by picking whichever has the higher safety
// rank. Ties (impossible for two distinct states under this ranking, kept
// as an explicit check rather than assumed) resolve to `recorded`.
TaskState ResolveTowardSafeState(TaskState recorded, TaskState reported);

}  // namespace fleet
