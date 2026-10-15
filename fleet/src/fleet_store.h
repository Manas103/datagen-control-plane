#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <pqxx/pqxx>

#include "task_state.h"

namespace fleet {

struct ReconciliationOutcome {
  std::string vehicle_id;
  std::string task_id;
  TaskState recorded_state;
  TaskState reported_state;
  TaskState resolved_state;
  bool changed = false;
};

// Owns the Postgres connection for the fleet task assignment domain: a
// separate table set (fleet_*) in the same database the sibling
// datagen control plane uses, reusing its idempotent check-then-act and
// crash-recovery methodology for a different domain (vehicle tasks
// instead of datagen jobs), not its schema.
class FleetStore {
 public:
  explicit FleetStore(const std::string& conninfo);

  // Drops and recreates every fleet_* table. Only ever called by
  // test/bench harnesses against a disposable database.
  void ResetSchema();

  // Idempotent on (vehicle_id, task_id): a second assignment of the same
  // pair is a no-op, never a second row and never an error. Returns true
  // if this call is the one that actually inserted the row.
  bool AssignTask(const std::string& vehicle_id, const std::string& task_id,
                   TaskState initial_state);

  // Enqueues a pending assignment for FleetCoordinator::DrainPending to
  // process; used by the crash-injection harness, which needs a queue a
  // separate, killable process can drain across restarts.
  void EnqueuePendingAssignment(const std::string& vehicle_id,
                                 const std::string& task_id,
                                 TaskState initial_state);

  // Drains every row currently in fleet_pending_assignments: for each,
  // idempotently assigns it (AssignTask) then removes it from the pending
  // queue. Safe to call repeatedly and safe to be killed mid-call: the
  // assignment is idempotent, and a row only leaves the pending queue
  // after its assignment is durably committed, so a kill before that
  // commit just means the row is retried on the next call, and a kill
  // after it but before the pending-row delete just means a harmless
  // repeat of an already-applied idempotent insert. Returns the number of
  // rows processed in this call.
  int DrainPending(int batch_limit = 1000);

  int64_t PendingCount();

  std::optional<TaskState> GetTaskState(const std::string& vehicle_id,
                                         const std::string& task_id);

  // Records a transition if and only if IsLegalTransition(current, to) is
  // true for the task's current recorded state. Returns false (no write)
  // for an illegal transition.
  bool RecordTransition(const std::string& vehicle_id,
                         const std::string& task_id, TaskState to);

  // Records a freshly reported telemetry observation and, if it disagrees
  // with the currently recorded state, resolves the disagreement toward
  // the safer of the two (ResolveTowardSafeState) and writes both a
  // fleet_reconciliations row and the resolved state back onto the task.
  // Returns std::nullopt if the task does not exist.
  std::optional<ReconciliationOutcome> ReportTelemetryAndReconcile(
      const std::string& vehicle_id, const std::string& task_id,
      TaskState reported);

  int64_t AssignedTaskCount();
  int64_t DistinctAssignedKeyCount();

 private:
  std::string conninfo_;
  pqxx::connection conn_;
};

}  // namespace fleet
