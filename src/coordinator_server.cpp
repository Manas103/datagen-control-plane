// Coordinator binary: a gRPC server exposing Submit/GetRequestState, plus a
// background dispatch thread that drains ready jobs under the fair-share
// policy and runs them to completion against Postgres. Started and killed
// by the crash-injection harness; `--epoch-from-db` makes a freshly
// (re)started process pick up a new epoch and run recovery before serving
// traffic, which is the exactly-once-under-crash mechanism this project
// claims.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "control_plane.grpc.pb.h"
#include "fair_share.h"
#include "store.h"

namespace {

std::atomic<bool> g_shutdown{false};

void HandleSignal(int) { g_shutdown.store(true); }

constexpr int kNumTenants = 8;

}  // namespace

// gRPC invokes service methods from its own internal thread pool, and a
// libpqxx connection is not safe to share across threads, so each handling
// thread gets its own lazily-constructed Store (own Postgres connection)
// rather than sharing one through a pointer, which is what the dispatch
// threads below do too.
class ControlPlaneServiceImpl final : public datagen::ControlPlane::Service {
 public:
  ControlPlaneServiceImpl(std::string conninfo)
      : conninfo_(std::move(conninfo)) {}

  grpc::Status Submit(grpc::ServerContext*, const datagen::SubmitRequest* req,
                      datagen::SubmitResponse* resp) override {
    datagen::SubmitOutcome outcome = StoreForThisThread().SubmitRequest(
        req->request_id(), req->tenant_id(), req->priority(),
        req->payload_kind());
    resp->set_admitted(outcome.admitted);
    resp->set_reason(outcome.reason);
    resp->set_job_count(outcome.job_count);
    return grpc::Status::OK;
  }

  grpc::Status GetRequestState(grpc::ServerContext*,
                                const datagen::RequestStateQuery* req,
                                datagen::RequestStateReply* resp) override {
    datagen::RequestState rs =
        StoreForThisThread().GetRequestState(req->request_id());
    resp->set_request_id(rs.request_id);
    resp->set_state(rs.state);
    for (const auto& job : rs.jobs) {
      auto* js = resp->add_jobs();
      js->set_job_id(job.job_id);
      js->set_state(job.state);
    }
    return grpc::Status::OK;
  }

 private:
  datagen::Store& StoreForThisThread() {
    thread_local std::unique_ptr<datagen::Store> store;
    if (!store) store = std::make_unique<datagen::Store>(conninfo_);
    return *store;
  }

  std::string conninfo_;
};

void DispatchLoop(int thread_index, const std::string& conninfo,
                   const std::string& side_effect_log_path, int64_t epoch,
                   bool fixed_ordering, datagen::FairShareScheduler* scheduler,
                   std::mutex* scheduler_mutex) {
  datagen::Store store(conninfo, side_effect_log_path);
  auto last_sweep = std::chrono::steady_clock::now() - std::chrono::hours(1);
  while (!g_shutdown.load()) {
    // SweepReadiness does a full scan of every pending job; running it from
    // all N dispatcher threads on every loop iteration (the first version)
    // meant N redundant full scans in flight continuously, which measured
    // as Submit() p99/p95 staying roughly flat (~23-31ms) even after the
    // admission path itself was cut from ~12 to ~3 round trips (see
    // README Findings): the bottleneck was never admission's own query
    // count, it was contention this created on Postgres from the dispatch
    // side. One thread, rate-limited to twice a second, is still far more
    // often than any single job takes to become ready in practice.
    auto now = std::chrono::steady_clock::now();
    if (thread_index == 0 &&
        now - last_sweep > std::chrono::milliseconds(500)) {
      store.SweepReadiness();
      last_sweep = now;
    }
    auto counts = store.ReadyCountsByTenant();
    if (counts.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      continue;
    }
    std::unordered_map<int, int> ready_map(counts.begin(), counts.end());
    int tenant;
    {
      std::lock_guard<std::mutex> lock(*scheduler_mutex);
      tenant = scheduler->PickNextTenant(ready_map);
    }
    if (tenant < 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      continue;
    }
    auto claimed = store.ClaimOneReadyJobForTenant(tenant, epoch);
    if (!claimed) continue;
    {
      std::lock_guard<std::mutex> lock(*scheduler_mutex);
      scheduler->NoteDispatch(tenant, claimed->priority);
    }
    store.ExecuteAndCommitJob(*claimed, epoch, fixed_ordering, nullptr);
  }
}

int main(int argc, char** argv) {
  std::string conninfo = "dbname=datagen_control user=datagen_app "
                          "password=datagen_dev_local host=localhost";
  std::string bind_address = "0.0.0.0:50061";
  std::string side_effect_log_path = "/tmp/datagen_side_effects.log";
  bool fixed_ordering = true;
  int64_t lease_ms = 500;
  int dispatch_threads = 6; // half of this machine's 12 WSL-capped cores

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto value_of = [&](const std::string& prefix) -> std::string {
      return arg.substr(prefix.size());
    };
    if (arg.rfind("--conninfo=", 0) == 0) conninfo = value_of("--conninfo=");
    else if (arg.rfind("--bind=", 0) == 0) bind_address = value_of("--bind=");
    else if (arg.rfind("--side-effect-log=", 0) == 0)
      side_effect_log_path = value_of("--side-effect-log=");
    else if (arg == "--buggy-ordering") fixed_ordering = false;
    else if (arg.rfind("--lease-ms=", 0) == 0)
      lease_ms = std::stoll(value_of("--lease-ms="));
    else if (arg.rfind("--dispatch-threads=", 0) == 0)
      dispatch_threads = std::stoi(value_of("--dispatch-threads="));
  }

  std::signal(SIGTERM, HandleSignal);
  std::signal(SIGINT, HandleSignal);

  int64_t epoch;
  int recovered;
  {
    // Startup-only Store: picks a fresh epoch and resolves/resets whatever
    // the previous (possibly SIGKILLed) coordinator left mid-flight, before
    // any dispatcher thread or RPC handler touches the database.
    datagen::Store startup_store(conninfo, side_effect_log_path);
    epoch = startup_store.NextEpoch();
    recovered = startup_store.RecoverStaleClaims(lease_ms);
  }
  std::cerr << "coordinator starting: epoch=" << epoch
            << " recovered_stale_claims=" << recovered
            << " fixed_ordering=" << fixed_ordering << std::endl;

  datagen::FairShareScheduler scheduler(kNumTenants);
  std::mutex scheduler_mutex;
  std::vector<std::thread> dispatchers;
  for (int i = 0; i < dispatch_threads; ++i) {
    dispatchers.emplace_back(DispatchLoop, i, conninfo, side_effect_log_path,
                              epoch, fixed_ordering, &scheduler,
                              &scheduler_mutex);
  }

  ControlPlaneServiceImpl service(conninfo);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cerr << "coordinator listening on " << bind_address << std::endl;

  std::thread waiter([&]() {
    while (!g_shutdown.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    server->Shutdown();
  });

  server->Wait();
  g_shutdown.store(true);
  for (auto& t : dispatchers) t.join();
  waiter.join();
  return 0;
}
