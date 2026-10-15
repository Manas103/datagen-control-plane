// A standalone, killable process that drains fleet_pending_assignments
// into fleet_tasks. Exists so the crash-injection harness has a real
// process to SIGKILL mid-work, the same way the sibling datagen
// coordinator is killed mid-dispatch; see fleet/README section of the
// top-level README for the exactly-once-under-crash claim this proves.
//
// Usage: fleet_coordinator <conninfo>
// Runs until fleet_pending_assignments is empty, then exits 0.

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "fleet_store.h"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <conninfo>\n", argv[0]);
    return 2;
  }
  std::string conninfo = argv[1];
  fleet::FleetStore store(conninfo);

  for (;;) {
    int processed = store.DrainPending(200);
    if (processed == 0) {
      if (store.PendingCount() == 0) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  std::printf("fleet_coordinator: drained, exiting\n");
  return 0;
}
