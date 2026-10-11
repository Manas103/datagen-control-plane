// Integration tests against a real local Postgres (datagen_control, see
// README for the one-time role/db setup). Each test resets the schema
// first, so tests are independent but not parallel-safe against each
// other; run this binary single-threaded (it is, by default, under
// GoogleTest).

#include <gtest/gtest.h>

#include <pqxx/pqxx>

#include "store.h"

namespace {
const char* kConn =
    "dbname=datagen_control user=datagen_app password=datagen_dev_local "
    "host=localhost";

}  // namespace

// pqxx::connection in this libpqxx version is neither copyable nor
// movable, so each test constructs its own datagen::Store in place
// (DATAGEN_FRESH_STORE) rather than returning one from a helper function.
#define DATAGEN_FRESH_STORE(name)         \
  datagen::Store name(kConn);             \
  name.ResetSchema()

TEST(Store, SubmitIsIdempotentOnRequestId) {
  DATAGEN_FRESH_STORE(store);
  auto a = store.SubmitRequest("req-idem-1", 2, 3, "ingest_scan");
  auto b = store.SubmitRequest("req-idem-1", 2, 3, "ingest_scan");
  EXPECT_TRUE(a.admitted);
  EXPECT_TRUE(b.admitted);
  EXPECT_EQ(a.job_count, b.job_count);

  pqxx::connection conn(kConn);
  pqxx::work txn(conn);
  pqxx::result requests_count =
      txn.exec("SELECT count(*) FROM requests WHERE request_id='req-idem-1'");
  EXPECT_EQ(requests_count[0][0].as<int>(), 1);
  pqxx::result transitions_count = txn.exec(
      "SELECT count(*) FROM transitions WHERE request_id='req-idem-1'");
  EXPECT_EQ(transitions_count[0][0].as<int>(), a.job_count);
}

TEST(Store, FullDrainOfASingleLinearRequest) {
  DATAGEN_FRESH_STORE(store);
  int64_t epoch = store.NextEpoch();
  std::string rid;
  for (int i = 0; i < 2000; ++i) {
    std::string candidate = "req-lin-" + std::to_string(i);
    if (datagen::TemplateIndexFor(candidate) == 0) {
      rid = candidate;
      break;
    }
  }
  ASSERT_FALSE(rid.empty());
  auto outcome = store.SubmitRequest(rid, 4, 3, "ingest_scan");
  ASSERT_EQ(outcome.job_count, 3);

  int guard = 0;
  while (!store.FullyDrained() && guard++ < 20) {
    auto counts = store.ReadyCountsByTenant();
    for (auto& [tenant, cnt] : counts) {
      auto claimed = store.ClaimOneReadyJobForTenant(tenant, epoch);
      if (claimed) {
        std::vector<std::string> log;
        store.ExecuteAndCommitJob(*claimed, epoch, true, &log);
      }
    }
  }
  EXPECT_TRUE(store.FullyDrained());
  auto state = store.GetRequestState(rid);
  EXPECT_EQ(state.state, "done");
  for (auto& job : state.jobs) EXPECT_EQ(job.state, "done");
}

TEST(Store, ExecuteAndCommitJobFiresWebhookExactlyOnceUnderDoubleInvocation) {
  DATAGEN_FRESH_STORE(store);
  int64_t epoch = store.NextEpoch();
  std::string rid;
  for (int i = 0; i < 2000; ++i) {
    std::string candidate = "req-webhook-" + std::to_string(i);
    if (datagen::TemplateIndexFor(candidate) == 0) {
      rid = candidate;
      break;
    }
  }
  store.SubmitRequest(rid, 1, 3, "ingest_scan");
  auto counts = store.ReadyCountsByTenant();
  auto claimed = store.ClaimOneReadyJobForTenant(1, epoch);
  ASSERT_TRUE(claimed);

  std::vector<std::string> log;
  store.ExecuteAndCommitJob(*claimed, epoch, /*fixed_ordering=*/true, &log);
  // A second, direct call simulates a stale claimant mistakenly re-running
  // the same already-claimed job under the same epoch (the condition the
  // webhook_log unique-insert guard exists to make harmless).
  store.ExecuteAndCommitJob(*claimed, epoch, /*fixed_ordering=*/true, &log);

  EXPECT_EQ(log.size(), 1u) << "fixed ordering must only fire the side "
                               "effect on the first commit";
}

TEST(Store, RecoverStaleClaimsResetsUnfinishedClaimToReady) {
  DATAGEN_FRESH_STORE(store);
  int64_t epoch = store.NextEpoch();
  std::string rid;
  for (int i = 0; i < 2000; ++i) {
    std::string candidate = "req-stale-" + std::to_string(i);
    if (datagen::TemplateIndexFor(candidate) == 0) {
      rid = candidate;
      break;
    }
  }
  store.SubmitRequest(rid, 5, 3, "ingest_scan");
  auto claimed = store.ClaimOneReadyJobForTenant(5, epoch);
  ASSERT_TRUE(claimed);
  // claimed but never executed (crash between claim and commit); webhook_log
  // has no row for this job, so recovery must reset it to ready, not mark
  // it done.
  int recovered = store.RecoverStaleClaims(/*lease_ms=*/0);
  EXPECT_GE(recovered, 1);
  auto state = store.GetRequestState(rid);
  bool any_ready = false;
  for (auto& job : state.jobs) {
    if (job.job_id == claimed->job_id) EXPECT_EQ(job.state, "ready");
    if (job.state == "ready") any_ready = true;
  }
  EXPECT_TRUE(any_ready);
}

TEST(Store, RecoverStaleClaimsResolvesDirectlyWhenWebhookAlreadyFired) {
  DATAGEN_FRESH_STORE(store);
  int64_t epoch = store.NextEpoch();
  std::string rid;
  for (int i = 0; i < 2000; ++i) {
    std::string candidate = "req-resolved-" + std::to_string(i);
    if (datagen::TemplateIndexFor(candidate) == 0) {
      rid = candidate;
      break;
    }
  }
  store.SubmitRequest(rid, 6, 3, "ingest_scan");
  auto claimed = store.ClaimOneReadyJobForTenant(6, epoch);
  ASSERT_TRUE(claimed);

  // Manually fake "the webhook fired, then the process died before the
  // mark-done commit": insert directly into webhook_log without calling
  // ExecuteAndCommitJob (which would also mark the job done).
  {
    pqxx::connection conn(kConn);
    pqxx::work txn(conn);
    txn.exec_params(
        "INSERT INTO webhook_log(request_id, job_id, epoch) VALUES "
        "($1,$2,$3)",
        claimed->request_id, claimed->job_id, epoch);
    txn.commit();
  }

  int recovered = store.RecoverStaleClaims(/*lease_ms=*/0);
  EXPECT_GE(recovered, 1);
  auto state = store.GetRequestState(rid);
  for (auto& job : state.jobs) {
    if (job.job_id == claimed->job_id) EXPECT_EQ(job.state, "done");
  }
}

TEST(Store, SweepReadinessPromotesAJobWhoseSiblingUpdatesWereNeverCrossChecked) {
  // Reproduces the race found under the real multi-threaded coordinator:
  // two sibling jobs (validate, enrich) each committed 'done' in their own
  // transaction without the other's PropagateReadiness call ever having
  // visibility into both at once, so neither promoted the dependent
  // 'merge' job. Built here by setting the two deps 'done' directly via
  // SQL (skipping ExecuteAndCommitJob's own propagation entirely) so the
  // test isolates SweepReadiness's own correctness rather than depending
  // on timing to reproduce the race.
  DATAGEN_FRESH_STORE(store);
  std::string rid;
  for (int i = 0; i < 2000; ++i) {
    std::string candidate = "req-race-" + std::to_string(i);
    if (datagen::TemplateIndexFor(candidate) == 2) {  // diamond template
      rid = candidate;
      break;
    }
  }
  ASSERT_FALSE(rid.empty());
  store.SubmitRequest(rid, 3, 3, "ingest_scan");

  {
    pqxx::connection conn(kConn);
    pqxx::work txn(conn);
    txn.exec_params(
        "UPDATE jobs SET state='done' WHERE request_id=$1 AND job_id IN "
        "('ingest','validate','enrich')",
        rid);
    txn.commit();
  }

  auto before = store.GetRequestState(rid);
  for (auto& job : before.jobs) {
    if (job.job_id == "merge") EXPECT_EQ(job.state, "pending");
  }

  int promoted = store.SweepReadiness();
  EXPECT_GE(promoted, 1);

  auto after = store.GetRequestState(rid);
  for (auto& job : after.jobs) {
    if (job.job_id == "merge") EXPECT_EQ(job.state, "ready");
    if (job.job_id == "publish") EXPECT_EQ(job.state, "pending");
  }
}

TEST(Store, StateDigestIsStableAcrossTwoIdenticallyDrivenRuns) {
  auto run_once = []() {
    DATAGEN_FRESH_STORE(store);
    int64_t epoch = store.NextEpoch();
    for (int i = 0; i < 30; ++i) {
      store.SubmitRequest("req-digest-" + std::to_string(i), i % 8, 3,
                           "ingest_scan");
    }
    int guard = 0;
    while (!store.FullyDrained() && guard++ < 200) {
      auto counts = store.ReadyCountsByTenant();
      for (auto& [tenant, cnt] : counts) {
        auto claimed = store.ClaimOneReadyJobForTenant(tenant, epoch);
        if (claimed) {
          std::vector<std::string> log;
          store.ExecuteAndCommitJob(*claimed, epoch, true, &log);
        }
      }
    }
    return store.ComputeStateDigest();
  };
  std::string digest_a = run_once();
  std::string digest_b = run_once();
  EXPECT_EQ(digest_a, digest_b);
}
