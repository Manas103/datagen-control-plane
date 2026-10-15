// CLI used by the crash-injection shell script: reset the schema, enqueue
// N pending assignments, and report counts after the coordinator (killed
// and restarted some number of times by the script) has drained them.
//
// Usage:
//   fleet_tool reset <conninfo>
//   fleet_tool enqueue <conninfo> <count>
//   fleet_tool counts <conninfo>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "fleet_store.h"

int main(int argc, char** argv) {
  if (argc < 3) {
    std::fprintf(stderr, "usage: %s <reset|enqueue|counts> <conninfo> [count]\n", argv[0]);
    return 2;
  }
  std::string cmd = argv[1];
  std::string conninfo = argv[2];
  fleet::FleetStore store(conninfo);

  if (cmd == "reset") {
    store.ResetSchema();
    std::printf("schema reset\n");
    return 0;
  }

  if (cmd == "enqueue") {
    if (argc < 4) {
      std::fprintf(stderr, "enqueue requires a count\n");
      return 2;
    }
    int count = std::atoi(argv[3]);
    for (int i = 0; i < count; ++i) {
      std::string vehicle_id = "veh-" + std::to_string(i);
      std::string task_id = "task-" + std::to_string(i);
      store.EnqueuePendingAssignment(vehicle_id, task_id, fleet::TaskState::kDispatch);
    }
    std::printf("enqueued %d pending assignments\n", count);
    return 0;
  }

  if (cmd == "counts") {
    std::printf("pending=%lld assigned=%lld distinct_assigned=%lld\n",
                 static_cast<long long>(store.PendingCount()),
                 static_cast<long long>(store.AssignedTaskCount()),
                 static_cast<long long>(store.DistinctAssignedKeyCount()));
    return 0;
  }

  std::fprintf(stderr, "unknown command: %s\n", cmd.c_str());
  return 2;
}
