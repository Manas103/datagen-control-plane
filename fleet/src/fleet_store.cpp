#include "fleet_store.h"

#include <stdexcept>

namespace fleet {

FleetStore::FleetStore(const std::string& conninfo)
    : conninfo_(conninfo), conn_(conninfo) {}

void FleetStore::ResetSchema() {
  pqxx::work txn(conn_);
  txn.exec(
      "DROP TABLE IF EXISTS fleet_reconciliations, fleet_telemetry_log, "
      "fleet_pending_assignments, fleet_transitions, fleet_tasks CASCADE;");
  txn.exec(
      "CREATE TABLE fleet_tasks ("
      " vehicle_id TEXT NOT NULL,"
      " task_id TEXT NOT NULL,"
      " state TEXT NOT NULL,"
      " assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
      " PRIMARY KEY(vehicle_id, task_id));");
  txn.exec(
      "CREATE TABLE fleet_pending_assignments ("
      " vehicle_id TEXT NOT NULL,"
      " task_id TEXT NOT NULL,"
      " initial_state TEXT NOT NULL,"
      " enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
      " PRIMARY KEY(vehicle_id, task_id));");
  txn.exec(
      "CREATE TABLE fleet_transitions ("
      " vehicle_id TEXT NOT NULL,"
      " task_id TEXT NOT NULL,"
      " seq INT NOT NULL,"
      " from_state TEXT NOT NULL,"
      " to_state TEXT NOT NULL,"
      " recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
      " PRIMARY KEY(vehicle_id, task_id, seq));");
  txn.exec(
      "CREATE TABLE fleet_telemetry_log ("
      " id BIGSERIAL PRIMARY KEY,"
      " vehicle_id TEXT NOT NULL,"
      " task_id TEXT NOT NULL,"
      " reported_state TEXT NOT NULL,"
      " reported_at TIMESTAMPTZ NOT NULL DEFAULT now());");
  txn.exec(
      "CREATE TABLE fleet_reconciliations ("
      " id BIGSERIAL PRIMARY KEY,"
      " vehicle_id TEXT NOT NULL,"
      " task_id TEXT NOT NULL,"
      " recorded_state TEXT NOT NULL,"
      " reported_state TEXT NOT NULL,"
      " resolved_state TEXT NOT NULL,"
      " resolved_at TIMESTAMPTZ NOT NULL DEFAULT now());");
  txn.commit();
}

bool FleetStore::AssignTask(const std::string& vehicle_id,
                              const std::string& task_id,
                              TaskState initial_state) {
  pqxx::work txn(conn_);
  pqxx::result ins = txn.exec_params(
      "INSERT INTO fleet_tasks(vehicle_id, task_id, state) VALUES "
      "($1,$2,$3) ON CONFLICT (vehicle_id, task_id) DO NOTHING RETURNING "
      "vehicle_id",
      vehicle_id, task_id, ToString(initial_state));
  bool inserted = !ins.empty();
  if (inserted) {
    txn.exec_params(
        "INSERT INTO fleet_transitions(vehicle_id, task_id, seq, "
        "from_state, to_state) VALUES ($1,$2,0,'none',$3) ON CONFLICT "
        "(vehicle_id, task_id, seq) DO NOTHING",
        vehicle_id, task_id, ToString(initial_state));
  }
  txn.commit();
  return inserted;
}

void FleetStore::EnqueuePendingAssignment(const std::string& vehicle_id,
                                            const std::string& task_id,
                                            TaskState initial_state) {
  pqxx::work txn(conn_);
  txn.exec_params(
      "INSERT INTO fleet_pending_assignments(vehicle_id, task_id, "
      "initial_state) VALUES ($1,$2,$3) ON CONFLICT (vehicle_id, task_id) "
      "DO NOTHING",
      vehicle_id, task_id, ToString(initial_state));
  txn.commit();
}

int FleetStore::DrainPending(int batch_limit) {
  pqxx::work select_txn(conn_);
  pqxx::result rows = select_txn.exec_params(
      "SELECT vehicle_id, task_id, initial_state FROM "
      "fleet_pending_assignments ORDER BY vehicle_id, task_id LIMIT $1",
      batch_limit);
  select_txn.commit();

  int processed = 0;
  for (const auto& row : rows) {
    const std::string vehicle_id = row[0].as<std::string>();
    const std::string task_id = row[1].as<std::string>();
    const std::string initial_state_str = row[2].as<std::string>();
    auto initial_state = FromString(initial_state_str);
    if (!initial_state) continue;

    // Idempotent regardless of whether this exact row was already
    // assigned by a coordinator instance that was killed after this
    // INSERT committed but before the DELETE below committed: the
    // ON CONFLICT DO NOTHING inside AssignTask makes re-running it on an
    // already-assigned (vehicle_id, task_id) a correct no-op, not a
    // duplicate.
    AssignTask(vehicle_id, task_id, *initial_state);

    pqxx::work del_txn(conn_);
    del_txn.exec_params(
        "DELETE FROM fleet_pending_assignments WHERE vehicle_id=$1 AND "
        "task_id=$2",
        vehicle_id, task_id);
    del_txn.commit();
    processed++;
  }
  return processed;
}

int64_t FleetStore::PendingCount() {
  pqxx::work txn(conn_);
  pqxx::result row = txn.exec("SELECT count(*) FROM fleet_pending_assignments");
  txn.commit();
  return row[0][0].as<int64_t>();
}

std::optional<TaskState> FleetStore::GetTaskState(
    const std::string& vehicle_id, const std::string& task_id) {
  pqxx::work txn(conn_);
  pqxx::result row = txn.exec_params(
      "SELECT state FROM fleet_tasks WHERE vehicle_id=$1 AND task_id=$2",
      vehicle_id, task_id);
  txn.commit();
  if (row.empty()) return std::nullopt;
  return FromString(row[0][0].as<std::string>());
}

bool FleetStore::RecordTransition(const std::string& vehicle_id,
                                    const std::string& task_id,
                                    TaskState to) {
  auto current = GetTaskState(vehicle_id, task_id);
  if (!current || !IsLegalTransition(*current, to)) return false;

  pqxx::work txn(conn_);
  pqxx::result updated = txn.exec_params(
      "UPDATE fleet_tasks SET state=$1 WHERE vehicle_id=$2 AND task_id=$3 "
      "AND state=$4 RETURNING vehicle_id",
      ToString(to), vehicle_id, task_id, ToString(*current));
  if (updated.empty()) {
    txn.abort();
    return false;
  }
  pqxx::result seq_row = txn.exec_params(
      "SELECT coalesce(max(seq),-1)+1 FROM fleet_transitions WHERE "
      "vehicle_id=$1 AND task_id=$2",
      vehicle_id, task_id);
  int next_seq = seq_row[0][0].as<int>();
  txn.exec_params(
      "INSERT INTO fleet_transitions(vehicle_id, task_id, seq, from_state, "
      "to_state) VALUES ($1,$2,$3,$4,$5)",
      vehicle_id, task_id, next_seq, ToString(*current), ToString(to));
  txn.commit();
  return true;
}

std::optional<ReconciliationOutcome> FleetStore::ReportTelemetryAndReconcile(
    const std::string& vehicle_id, const std::string& task_id,
    TaskState reported) {
  auto recorded = GetTaskState(vehicle_id, task_id);
  if (!recorded) return std::nullopt;

  pqxx::work log_txn(conn_);
  log_txn.exec_params(
      "INSERT INTO fleet_telemetry_log(vehicle_id, task_id, reported_state) "
      "VALUES ($1,$2,$3)",
      vehicle_id, task_id, ToString(reported));
  log_txn.commit();

  ReconciliationOutcome outcome;
  outcome.vehicle_id = vehicle_id;
  outcome.task_id = task_id;
  outcome.recorded_state = *recorded;
  outcome.reported_state = reported;

  if (*recorded == reported) {
    outcome.resolved_state = *recorded;
    outcome.changed = false;
    return outcome;
  }

  TaskState resolved = ResolveTowardSafeState(*recorded, reported);
  outcome.resolved_state = resolved;
  outcome.changed = (resolved != *recorded);

  pqxx::work txn(conn_);
  txn.exec_params(
      "INSERT INTO fleet_reconciliations(vehicle_id, task_id, "
      "recorded_state, reported_state, resolved_state) VALUES "
      "($1,$2,$3,$4,$5)",
      vehicle_id, task_id, ToString(*recorded), ToString(reported),
      ToString(resolved));
  if (outcome.changed) {
    txn.exec_params(
        "UPDATE fleet_tasks SET state=$1 WHERE vehicle_id=$2 AND "
        "task_id=$3",
        ToString(resolved), vehicle_id, task_id);
  }
  txn.commit();
  return outcome;
}

int64_t FleetStore::AssignedTaskCount() {
  pqxx::work txn(conn_);
  pqxx::result row = txn.exec("SELECT count(*) FROM fleet_tasks");
  txn.commit();
  return row[0][0].as<int64_t>();
}

int64_t FleetStore::DistinctAssignedKeyCount() {
  pqxx::work txn(conn_);
  pqxx::result row =
      txn.exec("SELECT count(DISTINCT (vehicle_id, task_id)) FROM fleet_tasks");
  txn.commit();
  return row[0][0].as<int64_t>();
}

}  // namespace fleet
