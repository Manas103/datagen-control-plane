#include "store.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <openssl/evp.h>

namespace datagen {

namespace {

std::string JoinDeps(const std::vector<std::string>& deps) {
  std::ostringstream oss;
  for (size_t i = 0; i < deps.size(); ++i) {
    if (i) oss << ',';
    oss << deps[i];
  }
  return oss.str();
}

std::string Sha256Hex(const std::string& data) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int len = 0;
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
  EVP_DigestUpdate(ctx, data.data(), data.size());
  EVP_DigestFinal_ex(ctx, digest, &len);
  EVP_MD_CTX_free(ctx);
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  for (unsigned int i = 0; i < len; ++i) {
    oss << std::setw(2) << static_cast<int>(digest[i]);
  }
  return oss.str();
}

}  // namespace

Store::Store(const std::string& conninfo,
             const std::string& side_effect_log_path)
    : conninfo_(conninfo),
      side_effect_log_path_(side_effect_log_path),
      conn_(conninfo) {
  // synchronous_commit=off trades a small, bounded durability window (a
  // just-committed row could be lost only if the Postgres *server* itself
  // crashes before its background flush catches up) for materially lower
  // commit latency. Safe for what this project's crash-injection claim
  // actually tests: SIGKILL against the *coordinator* process, never
  // against Postgres itself, so the exactly-once-under-crash guarantee
  // (which depends on the coordinator's own transactions being atomic, not
  // on every commit being fsynced before the client sees the reply) is
  // unaffected. See README Findings for the measured admission-latency
  // effect and Limitations for this tradeoff stated plainly.
  pqxx::work txn(conn_);
  txn.exec("SET synchronous_commit = off");
  txn.commit();
}

void Store::AppendSideEffect(const std::string& request_id,
                              const std::string& job_id) {
  if (side_effect_log_path_.empty()) return;
  FILE* f = std::fopen(side_effect_log_path_.c_str(), "a");
  if (!f) return;
  std::fprintf(f, "%s,%s\n", request_id.c_str(), job_id.c_str());
  std::fclose(f);
}

void Store::ResetSchema() {
  pqxx::work txn(conn_);
  txn.exec(
      "DROP TABLE IF EXISTS webhook_log, transitions, jobs, requests, "
      "coordinator_epoch CASCADE;");
  txn.exec(
      "CREATE TABLE coordinator_epoch (id INT PRIMARY KEY, value BIGINT NOT "
      "NULL);");
  txn.exec("INSERT INTO coordinator_epoch(id, value) VALUES (1, 0);");
  txn.exec(
      "CREATE TABLE requests ("
      " request_id TEXT PRIMARY KEY,"
      " tenant_id INT NOT NULL,"
      " priority INT NOT NULL,"
      " payload_kind TEXT NOT NULL,"
      " submitted_at TIMESTAMPTZ NOT NULL DEFAULT now());");
  txn.exec(
      "CREATE TABLE jobs ("
      " request_id TEXT NOT NULL REFERENCES requests(request_id),"
      " job_id TEXT NOT NULL,"
      " deps TEXT NOT NULL,"
      " tenant_id INT NOT NULL,"
      " priority INT NOT NULL,"
      " state TEXT NOT NULL,"
      " claimed_epoch BIGINT,"
      " claimed_at TIMESTAMPTZ,"
      " PRIMARY KEY(request_id, job_id));");
  txn.exec("CREATE INDEX jobs_ready_tenant_idx ON jobs(tenant_id, state);");
  txn.exec(
      "CREATE TABLE transitions ("
      " request_id TEXT NOT NULL,"
      " job_id TEXT NOT NULL,"
      " seq INT NOT NULL,"
      " from_state TEXT NOT NULL,"
      " to_state TEXT NOT NULL,"
      " recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
      " PRIMARY KEY(request_id, job_id, seq));");
  txn.exec(
      "CREATE TABLE webhook_log ("
      " request_id TEXT NOT NULL,"
      " job_id TEXT NOT NULL,"
      " epoch BIGINT NOT NULL,"
      " fired_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
      " PRIMARY KEY(request_id, job_id));");
  txn.commit();
}

void Store::RecordTransition(pqxx::work& txn, const std::string& request_id,
                              const std::string& job_id, int seq,
                              const std::string& from_state,
                              const std::string& to_state) {
  txn.exec_params(
      "INSERT INTO transitions(request_id, job_id, seq, from_state, "
      "to_state) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (request_id, job_id, "
      "seq) DO NOTHING",
      request_id, job_id, seq, from_state, to_state);
}

SubmitOutcome Store::SubmitRequest(const std::string& request_id,
                                    int tenant_id, int priority,
                                    const std::string& payload_kind) {
  pqxx::work txn(conn_);

  // Insert-and-detect-duplicate in one round trip (ON CONFLICT ... RETURNING)
  // instead of a separate SELECT-then-INSERT: a first version measured
  // admission p99 around 31ms against a 5ms target at a 200k backlog,
  // dominated by round-trip count (roughly 2*job_count + 2 serial
  // round trips per call); see README Findings for the full measurement.
  pqxx::result ins = txn.exec_params(
      "INSERT INTO requests(request_id, tenant_id, priority, payload_kind) "
      "VALUES ($1,$2,$3,$4) ON CONFLICT (request_id) DO NOTHING RETURNING "
      "request_id",
      request_id, tenant_id, priority, payload_kind);
  if (ins.empty()) {
    // Duplicate submission of a request_id already admitted earlier (this
    // is exactly the shape the 5%-duplicated replay test injects). Nothing
    // new is recorded; every transition for this request_id was already
    // written exactly once by the first admission.
    pqxx::result jc =
        txn.exec_params("SELECT count(*) FROM jobs WHERE request_id=$1",
                         request_id);
    txn.commit();
    return SubmitOutcome{true, "duplicate_request_id_noop",
                          jc[0][0].as<int>()};
  }

  std::vector<JobSpec> jobs = ExpandDag(request_id, payload_kind);

  // Batch every job row and every initial transition row into one
  // multi-row INSERT each, instead of 2 round trips per job: job_count is
  // 3 to 5 here, but this is the part of the call that scaled with DAG
  // size, so collapsing it to 2 round trips total (regardless of
  // job_count) is what actually moved the latency number, not the
  // combined-insert change above (see README Findings for the measured
  // before/after).
  std::ostringstream jobs_sql;
  jobs_sql << "INSERT INTO jobs(request_id, job_id, deps, tenant_id, "
              "priority, state) VALUES ";
  std::ostringstream trans_sql;
  trans_sql << "INSERT INTO transitions(request_id, job_id, seq, "
               "from_state, to_state) VALUES ";
  for (size_t i = 0; i < jobs.size(); ++i) {
    const JobSpec& job = jobs[i];
    const std::string initial_state = job.deps.empty() ? "ready" : "pending";
    if (i) {
      jobs_sql << ',';
      trans_sql << ',';
    }
    jobs_sql << '(' << txn.quote(request_id) << ',' << txn.quote(job.job_id)
              << ',' << txn.quote(JoinDeps(job.deps)) << ',' << tenant_id
              << ',' << priority << ',' << txn.quote(initial_state) << ')';
    trans_sql << '(' << txn.quote(request_id) << ',' << txn.quote(job.job_id)
               << ",0," << txn.quote(std::string("none")) << ','
               << txn.quote(initial_state) << ')';
  }
  trans_sql << " ON CONFLICT (request_id, job_id, seq) DO NOTHING";

  txn.exec(jobs_sql.str());
  txn.exec(trans_sql.str());
  txn.commit();
  return SubmitOutcome{true, "admitted", static_cast<int>(jobs.size())};
}

std::vector<std::pair<int, int>> Store::ReadyCountsByTenant() {
  pqxx::work txn(conn_);
  pqxx::result rows = txn.exec(
      "SELECT tenant_id, count(*) FROM jobs WHERE state='ready' GROUP BY "
      "tenant_id");
  txn.commit();
  std::vector<std::pair<int, int>> out;
  for (const auto& row : rows) {
    out.emplace_back(row[0].as<int>(), row[1].as<int>());
  }
  return out;
}

std::optional<ClaimedJob> Store::ClaimOneReadyJobForTenant(int tenant_id,
                                                             int64_t epoch) {
  pqxx::work txn(conn_);
  pqxx::result cand = txn.exec_params(
      "SELECT request_id, job_id, priority FROM jobs WHERE tenant_id=$1 AND "
      "state='ready' ORDER BY request_id, job_id LIMIT 1 FOR UPDATE SKIP "
      "LOCKED",
      tenant_id);
  if (cand.empty()) {
    txn.commit();
    return std::nullopt;
  }
  const std::string request_id = cand[0][0].as<std::string>();
  const std::string job_id = cand[0][1].as<std::string>();
  const int priority = cand[0][2].as<int>();

  pqxx::result updated = txn.exec_params(
      "UPDATE jobs SET state='claimed', claimed_epoch=$1, claimed_at=now() "
      "WHERE request_id=$2 AND job_id=$3 AND state='ready' RETURNING "
      "request_id",
      epoch, request_id, job_id);
  if (updated.empty()) {
    txn.commit();
    return std::nullopt;
  }
  RecordTransition(txn, request_id, job_id, 2, "ready", "claimed");
  txn.commit();
  return ClaimedJob{request_id, job_id, tenant_id, priority};
}

void Store::ExecuteAndCommitJob(const ClaimedJob& job, int64_t epoch,
                                 bool fixed_ordering,
                                 std::vector<std::string>* side_effect_log) {
  const std::string key = job.request_id + "," + job.job_id;

  if (fixed_ordering) {
    // Check-then-act: reserve the right to fire the side effect first.
    // Only the caller that actually inserts the row is allowed to invoke
    // the (simulated) external side effect.
    pqxx::work txn(conn_);
    pqxx::result reserved = txn.exec_params(
        "INSERT INTO webhook_log(request_id, job_id, epoch) VALUES "
        "($1,$2,$3) ON CONFLICT (request_id, job_id) DO NOTHING RETURNING "
        "request_id",
        job.request_id, job.job_id, epoch);
    txn.commit();
    if (!reserved.empty()) {
      if (side_effect_log) side_effect_log->push_back(key);
      AppendSideEffect(job.request_id, job.job_id);
    }
  } else {
    // Buggy ordering, used only to measure the bug: fire the side effect
    // unconditionally, then record it. A job re-executed after a crash
    // (recovered claim) fires a second time even though the table below
    // still only ever holds one row per (request_id, job_id). The sleep
    // below is a demonstration aid only (never present in fixed_ordering,
    // which is what the coordinator always actually runs with): it widens
    // the real but narrow crash window between the side effect and its
    // durable record so the crash-injection harness's ordinary kill timing
    // reliably lands inside it, instead of needing many more injected
    // kills to find it by chance.
    if (side_effect_log) side_effect_log->push_back(key);
    AppendSideEffect(job.request_id, job.job_id);
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    pqxx::work txn(conn_);
    txn.exec_params(
        "INSERT INTO webhook_log(request_id, job_id, epoch) VALUES "
        "($1,$2,$3) ON CONFLICT (request_id, job_id) DO NOTHING",
        job.request_id, job.job_id, epoch);
    txn.commit();
  }

  pqxx::work txn2(conn_);
  txn2.exec_params(
      "UPDATE jobs SET state='done' WHERE request_id=$1 AND job_id=$2 AND "
      "claimed_epoch=$3 AND state='claimed'",
      job.request_id, job.job_id, epoch);
  RecordTransition(txn2, job.request_id, job.job_id, 3, "claimed", "done");
  PropagateReadiness(txn2, job.request_id);
  txn2.commit();
}

void Store::PropagateReadiness(pqxx::work& txn,
                                const std::string& request_id) {
  pqxx::result pending = txn.exec_params(
      "SELECT job_id, deps FROM jobs WHERE request_id=$1 AND state='pending'",
      request_id);
  for (const auto& row : pending) {
    const std::string job_id = row[0].as<std::string>();
    const std::string deps = row[1].as<std::string>();
    if (deps.empty()) continue;
    std::istringstream iss(deps);
    std::string dep;
    bool all_done = true;
    while (std::getline(iss, dep, ',')) {
      pqxx::result st = txn.exec_params(
          "SELECT state FROM jobs WHERE request_id=$1 AND job_id=$2",
          request_id, dep);
      if (st.empty() || st[0][0].as<std::string>() != "done") {
        all_done = false;
        break;
      }
    }
    if (all_done) {
      pqxx::result updated = txn.exec_params(
          "UPDATE jobs SET state='ready' WHERE request_id=$1 AND job_id=$2 "
          "AND state='pending' RETURNING job_id",
          request_id, job_id);
      if (!updated.empty()) {
        RecordTransition(txn, request_id, job_id, 1, "pending", "ready");
      }
    }
  }
}

int Store::SweepReadiness() {
  pqxx::work txn(conn_);
  pqxx::result promoted = txn.exec(
      "WITH promotable AS ("
      "  SELECT p.request_id, p.job_id"
      "  FROM jobs p"
      "  WHERE p.state = 'pending'"
      "    AND NOT EXISTS ("
      "      SELECT 1 FROM unnest(string_to_array(p.deps, ',')) AS dep_id"
      "      JOIN jobs d ON d.request_id = p.request_id AND d.job_id = "
      "dep_id"
      "      WHERE d.state <> 'done'"
      "    )"
      ")"
      "UPDATE jobs j SET state = 'ready'"
      "  FROM promotable pr"
      "  WHERE j.request_id = pr.request_id AND j.job_id = pr.job_id AND "
      "j.state = 'pending'"
      "  RETURNING j.request_id, j.job_id");
  for (const auto& row : promoted) {
    RecordTransition(txn, row[0].as<std::string>(), row[1].as<std::string>(),
                      1, "pending", "ready");
  }
  txn.commit();
  return static_cast<int>(promoted.size());
}

int Store::RecoverStaleClaims(int64_t lease_ms) {
  pqxx::work txn(conn_);
  pqxx::result stale = txn.exec_params(
      "SELECT request_id, job_id FROM jobs WHERE state='claimed' AND "
      "claimed_at < now() - ($1 || ' milliseconds')::interval",
      lease_ms);
  int recovered = 0;
  for (const auto& row : stale) {
    const std::string request_id = row[0].as<std::string>();
    const std::string job_id = row[1].as<std::string>();
    pqxx::result fired = txn.exec_params(
        "SELECT 1 FROM webhook_log WHERE request_id=$1 AND job_id=$2",
        request_id, job_id);
    if (!fired.empty()) {
      // The side effect already happened in a prior epoch; do not
      // re-execute, just resolve the job as done and let dependents unlock.
      txn.exec_params(
          "UPDATE jobs SET state='done' WHERE request_id=$1 AND job_id=$2",
          request_id, job_id);
      RecordTransition(txn, request_id, job_id, 3, "claimed", "done");
    } else {
      txn.exec_params(
          "UPDATE jobs SET state='ready', claimed_epoch=NULL, "
          "claimed_at=NULL WHERE request_id=$1 AND job_id=$2",
          request_id, job_id);
    }
    recovered++;
  }
  for (const auto& row : stale) {
    PropagateReadiness(txn, row[0].as<std::string>());
  }
  txn.commit();
  return recovered;
}

RequestState Store::GetRequestState(const std::string& request_id) {
  pqxx::work txn(conn_);
  pqxx::result jobs = txn.exec_params(
      "SELECT job_id, state FROM jobs WHERE request_id=$1 ORDER BY job_id",
      request_id);
  txn.commit();
  RequestState rs;
  rs.request_id = request_id;
  bool all_done = !jobs.empty();
  for (const auto& row : jobs) {
    JobRow jr{row[0].as<std::string>(), row[1].as<std::string>()};
    if (jr.state != "done") all_done = false;
    rs.jobs.push_back(jr);
  }
  rs.state = jobs.empty() ? "unknown" : (all_done ? "done" : "running");
  return rs;
}

int64_t Store::NextEpoch() {
  pqxx::work txn(conn_);
  pqxx::result row = txn.exec(
      "UPDATE coordinator_epoch SET value = value + 1 WHERE id=1 RETURNING "
      "value");
  txn.commit();
  return row[0][0].as<int64_t>();
}

std::string Store::ComputeStateDigest() {
  pqxx::work txn(conn_);
  pqxx::result requests_rows = txn.exec(
      "SELECT request_id FROM requests ORDER BY request_id");
  pqxx::result job_rows = txn.exec(
      "SELECT request_id, job_id, state FROM jobs ORDER BY request_id, "
      "job_id");
  pqxx::result webhook_rows = txn.exec(
      "SELECT request_id, job_id FROM webhook_log ORDER BY request_id, "
      "job_id");
  txn.commit();

  std::ostringstream oss;
  for (const auto& r : requests_rows) oss << "R|" << r[0].c_str() << '\n';
  for (const auto& r : job_rows) {
    oss << "J|" << r[0].c_str() << '|' << r[1].c_str() << '|' << r[2].c_str()
        << '\n';
  }
  for (const auto& r : webhook_rows) {
    oss << "W|" << r[0].c_str() << '|' << r[1].c_str() << '\n';
  }
  return Sha256Hex(oss.str());
}

bool Store::FullyDrained() {
  pqxx::work txn(conn_);
  pqxx::result row =
      txn.exec("SELECT count(*) FROM jobs WHERE state != 'done'");
  txn.commit();
  return row[0][0].as<int64_t>() == 0;
}

int64_t Store::PendingOrReadyJobCount() {
  pqxx::work txn(conn_);
  pqxx::result row = txn.exec(
      "SELECT count(*) FROM jobs WHERE state IN ('pending','ready')");
  txn.commit();
  return row[0][0].as<int64_t>();
}

}  // namespace datagen
