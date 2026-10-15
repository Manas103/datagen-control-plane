#include "task_state.h"

#include <stdexcept>
#include <unordered_map>

namespace fleet {

std::string ToString(TaskState s) {
  switch (s) {
    case TaskState::kDispatch: return "dispatch";
    case TaskState::kEnRoute: return "en_route";
    case TaskState::kOnTrip: return "on_trip";
    case TaskState::kCharging: return "charging";
    case TaskState::kCleaning: return "cleaning";
    case TaskState::kOutOfService: return "out_of_service";
  }
  throw std::invalid_argument("unreachable TaskState");
}

std::optional<TaskState> FromString(const std::string& s) {
  static const std::unordered_map<std::string, TaskState> kByName = {
      {"dispatch", TaskState::kDispatch},
      {"en_route", TaskState::kEnRoute},
      {"on_trip", TaskState::kOnTrip},
      {"charging", TaskState::kCharging},
      {"cleaning", TaskState::kCleaning},
      {"out_of_service", TaskState::kOutOfService},
  };
  auto it = kByName.find(s);
  if (it == kByName.end()) return std::nullopt;
  return it->second;
}

bool IsLegalTransition(TaskState from, TaskState to) {
  using S = TaskState;
  switch (from) {
    case S::kDispatch:
      return to == S::kEnRoute || to == S::kOutOfService;
    case S::kEnRoute:
      return to == S::kOnTrip || to == S::kOutOfService;
    case S::kOnTrip:
      return to == S::kCharging || to == S::kCleaning || to == S::kOutOfService;
    case S::kCharging:
      return to == S::kDispatch || to == S::kOutOfService;
    case S::kCleaning:
      return to == S::kDispatch || to == S::kOutOfService;
    case S::kOutOfService:
      return false;  // terminal for this task
  }
  return false;
}

int SafetyRank(TaskState s) {
  switch (s) {
    case TaskState::kOutOfService: return 5;
    case TaskState::kCleaning: return 4;
    case TaskState::kCharging: return 3;
    case TaskState::kDispatch: return 2;
    case TaskState::kEnRoute: return 1;
    case TaskState::kOnTrip: return 0;
  }
  return -1;
}

TaskState ResolveTowardSafeState(TaskState recorded, TaskState reported) {
  if (SafetyRank(reported) > SafetyRank(recorded)) return reported;
  return recorded;
}

}  // namespace fleet
