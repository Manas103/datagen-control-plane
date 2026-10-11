#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <pqxx/pqxx>

#include "dag.h"

namespace datagen {

struct JobRow {
  std::string job_id;
  std::string state;
};

struct RequestState {
  std::string request_id;
  std::string state; // derived: PENDING, RUNNING, DONE
  std::vector<JobRow> jobs;
};

struct SubmitOutcome {
  bool admitted = false;
  std::string reason;
  int job_count = 0;
};

struct ClaimedJob {
  std::string request_id;
  std::string job_id;
  int tenant_id = 0;
  int priority = 0;
};

// Owns the Postgres connection and every SQL statement the coordinator
// issues. Kept deliberately free of gRPC and of the fair-share policy so
// each can be unit tested independently (store against a real local
// Postgres, policy and DAG expansion in-process with no I/O).
class Store {
 public:
  // side_effect_log_path, when non-empty, is opened in append mode on every
  // fired side effect so the write survives this process being killed a
  // moment later; this is what lets the crash-injection harness detect a
  // duplicate side effect across a coordinator restart. Empty means
  // in-memory only (used by unit tests that do not crash anything).
  explicit Store(const std::string& conninfo,
                 const std::string& side_effect_log_path = "");

  // Drops and recreates every table. Only ever called by test/bench
  // harnesses against a disposable database, never by the coordinator.
  void ResetSchema();

  // Idempotent: submitting the same request_id twice (even concurrently)
  // results in exactly one requests row, one jobs row per DAG node, and one
  // transition row per (request_id, job_id, seq). This is what the 5%
  // duplicated replay test exercises directly.
  SubmitOutcome SubmitRequest(const std::string& request_id, int tenant_id,
                               int priority, const std::string& payload_kind);

  // Returns up to `limit` ready jobs per tenant_id (job counts only), used
  // by the dispatcher to ask the fair-share policy which tenant goes next
  // without having to materialize every ready job up front.
  std::vector<std::pair<int, int>> ReadyCountsByTenant();

  // Picks one ready job belonging to the given tenant (oldest by submit
  // order) and atomically claims it under the given coordinator epoch.
  // Returns std::nullopt if no ready job for that tenant exists any more
  // (lost a race with a concurrent claim, or already drained).
  std::optional<ClaimedJob> ClaimOneReadyJobForTenant(int tenant_id,
                                                       int64_t epoch);

  // The idempotency-guarded "external side effect." `fixed_ordering`
  // selects between the buggy and corrected sequencing described in the
  // README Findings section; the coordinator always runs with
  // fixed_ordering = true, the crash-injection harness runs once with
  // false to measure the bug before it was fixed.
  void ExecuteAndCommitJob(const ClaimedJob& job, int64_t epoch,
                            bool fixed_ordering,
                            std::vector<std::string>* side_effect_log);

  // Finds jobs stuck in claimed/executing past `lease_ms` and either
  // resolves them directly (webhook already fired) or resets them to ready
  // for reclaiming. Called on coordinator startup and periodically.
  int RecoverStaleClaims(int64_t lease_ms);

  RequestState GetRequestState(const std::string& request_id);

  int64_t NextEpoch();

  // SHA-256 (hex) over a canonical, sorted text dump of requests, jobs,
  // transitions and webhook_log. Two databases reaching the same digest
  // have reached an identical final state by construction, not by
  // sampling.
  std::string ComputeStateDigest();

  // True once every job of every submitted request is in state 'done'.
  bool FullyDrained();

  int64_t PendingOrReadyJobCount();

  // Global, statement-atomic readiness promotion: pending -> ready for
  // every job whose dependencies are all done, across every request, in
  // one SQL statement. Called by every dispatcher thread on every loop
  // iteration as a safety net; see README Findings for why the
  // per-completion PropagateReadiness below is not sufficient on its own
  // (two sibling jobs completing in overlapping transactions can each miss
  // the other's not-yet-committed completion and neither promotes the
  // dependent job). Returns the number of jobs promoted.
  int SweepReadiness();

 private:
  void PropagateReadiness(pqxx::work& txn, const std::string& request_id);
  void RecordTransition(pqxx::work& txn, const std::string& request_id,
                         const std::string& job_id, int seq,
                         const std::string& from_state,
                         const std::string& to_state);

  void AppendSideEffect(const std::string& request_id,
                        const std::string& job_id);

  std::string conninfo_;
  std::string side_effect_log_path_;
  // One real TCP connection per Store instance, held open for the
  // instance's lifetime. A first version opened a fresh connection on
  // every call; that measured at roughly 20ms of admission latency per
  // call (connection setup, not the query) against a claimed 5ms budget.
  // Each thread that touches Postgres (every dispatcher thread, and every
  // gRPC handler thread via a thread_local Store) owns its own Store and
  // therefore its own connection; libpqxx connections are not safe to
  // share across threads.
  pqxx::connection conn_;
};

}  // namespace datagen
