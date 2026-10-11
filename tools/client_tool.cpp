// Multi-purpose CLI driving the coordinator over real gRPC (for admission)
// and reading Postgres directly through Store (for drain/digest checks),
// exactly the way the bench and replay-digest harnesses need to.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "control_plane.grpc.pb.h"
#include "store.h"

namespace {

std::string ArgOr(const std::vector<std::string>& args,
                   const std::string& key, const std::string& def) {
  const std::string prefix = "--" + key + "=";
  for (const auto& a : args) {
    if (a.rfind(prefix, 0) == 0) return a.substr(prefix.size());
  }
  return def;
}

struct Row {
  std::string request_id;
  int tenant_id;
  int priority;
  std::string payload_kind;
};

std::vector<Row> ReadCsv(const std::string& path) {
  std::vector<Row> rows;
  std::ifstream f(path);
  std::string line;
  while (std::getline(f, line)) {
    if (line.empty()) continue;
    std::istringstream iss(line);
    std::string rid, tid, pr, kind;
    std::getline(iss, rid, ',');
    std::getline(iss, tid, ',');
    std::getline(iss, pr, ',');
    std::getline(iss, kind, ',');
    rows.push_back({rid, std::stoi(tid), std::stoi(pr), kind});
  }
  return rows;
}

double PercentileMs(std::vector<double>& sorted_us, double pct) {
  if (sorted_us.empty()) return 0.0;
  size_t idx = static_cast<size_t>(pct / 100.0 * (sorted_us.size() - 1));
  return sorted_us[idx] / 1000.0;
}

int CmdBulk(const std::vector<std::string>& args) {
  const std::string server = ArgOr(args, "server", "localhost:50061");
  const std::string file = ArgOr(args, "file", "");
  const std::string out = ArgOr(args, "out", "");
  const int threads = std::stoi(ArgOr(args, "threads", "4"));

  std::vector<Row> rows = ReadCsv(file);
  std::vector<std::vector<double>> latencies_by_thread(threads);
  std::atomic<int64_t> admitted_count{0};

  auto worker = [&](int t) {
    auto channel =
        grpc::CreateChannel(server, grpc::InsecureChannelCredentials());
    auto stub = datagen::ControlPlane::NewStub(channel);
    for (size_t i = t; i < rows.size(); i += threads) {
      const Row& row = rows[i];
      datagen::SubmitRequest req;
      req.set_request_id(row.request_id);
      req.set_tenant_id(row.tenant_id);
      req.set_priority(row.priority);
      req.set_payload_kind(row.payload_kind);
      datagen::SubmitResponse resp;
      grpc::ClientContext ctx;
      auto t0 = std::chrono::steady_clock::now();
      grpc::Status status = stub->Submit(&ctx, req, &resp);
      auto t1 = std::chrono::steady_clock::now();
      double us =
          std::chrono::duration<double, std::micro>(t1 - t0).count();
      latencies_by_thread[t].push_back(us);
      if (status.ok() && resp.admitted()) admitted_count++;
    }
  };

  std::vector<std::thread> workers;
  auto start = std::chrono::steady_clock::now();
  for (int t = 0; t < threads; ++t) workers.emplace_back(worker, t);
  for (auto& w : workers) w.join();
  auto end = std::chrono::steady_clock::now();

  std::vector<double> all;
  for (auto& v : latencies_by_thread)
    all.insert(all.end(), v.begin(), v.end());
  std::sort(all.begin(), all.end());

  const double wall_s =
      std::chrono::duration<double>(end - start).count();
  std::cout << "submitted=" << rows.size() << " admitted=" << admitted_count
            << " wall_seconds=" << wall_s << "\n";
  std::cout << "admission_latency_ms p50=" << PercentileMs(all, 50)
            << " p95=" << PercentileMs(all, 95)
            << " p99=" << PercentileMs(all, 99)
            << " max=" << PercentileMs(all, 100) << "\n";

  if (!out.empty()) {
    std::ofstream of(out);
    for (double v : all) of << v << "\n";
  }
  return 0;
}

int CmdSubmitOne(const std::vector<std::string>& args) {
  const std::string server = ArgOr(args, "server", "localhost:50061");
  auto channel =
      grpc::CreateChannel(server, grpc::InsecureChannelCredentials());
  auto stub = datagen::ControlPlane::NewStub(channel);
  datagen::SubmitRequest req;
  req.set_request_id(ArgOr(args, "request_id", "req-manual-0"));
  req.set_tenant_id(std::stoi(ArgOr(args, "tenant", "0")));
  req.set_priority(std::stoi(ArgOr(args, "priority", "3")));
  req.set_payload_kind(ArgOr(args, "kind", "ingest_scan"));
  datagen::SubmitResponse resp;
  grpc::ClientContext ctx;
  grpc::Status status = stub->Submit(&ctx, req, &resp);
  std::cout << "ok=" << status.ok() << " admitted=" << resp.admitted()
            << " reason=" << resp.reason() << " jobs=" << resp.job_count()
            << "\n";
  return status.ok() ? 0 : 1;
}

int CmdReset(const std::vector<std::string>& args) {
  datagen::Store store(ArgOr(args, "conninfo", "dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"));
  store.ResetSchema();
  std::cout << "schema reset\n";
  return 0;
}

int CmdDigest(const std::vector<std::string>& args) {
  datagen::Store store(ArgOr(args, "conninfo", "dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"));
  std::cout << store.ComputeStateDigest() << "\n";
  return 0;
}

int CmdPending(const std::vector<std::string>& args) {
  datagen::Store store(ArgOr(args, "conninfo", "dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"));
  std::cout << store.PendingOrReadyJobCount() << "\n";
  return 0;
}

int CmdDrain(const std::vector<std::string>& args) {
  datagen::Store store(ArgOr(args, "conninfo", "dbname=datagen_control user=datagen_app password=datagen_dev_local host=localhost"));
  const int timeout_s = std::stoi(ArgOr(args, "timeout_s", "600"));
  const int poll_ms = std::stoi(ArgOr(args, "poll_ms", "500"));
  auto start = std::chrono::steady_clock::now();
  while (true) {
    if (store.FullyDrained()) {
      std::cout << "drained after "
                << std::chrono::duration<double>(
                       std::chrono::steady_clock::now() - start)
                       .count()
                << "s\n";
      return 0;
    }
    if (std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                       start)
            .count() > timeout_s) {
      std::cout << "timeout, pending=" << store.PendingOrReadyJobCount()
                << "\n";
      return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: control_plane_cli <reset|submit|bulk|digest|"
                 "pending|drain> [--key=value ...]\n";
    return 1;
  }
  std::string cmd = argv[1];
  std::vector<std::string> args(argv + 2, argv + argc);

  if (cmd == "reset") return CmdReset(args);
  if (cmd == "submit") return CmdSubmitOne(args);
  if (cmd == "bulk") return CmdBulk(args);
  if (cmd == "digest") return CmdDigest(args);
  if (cmd == "pending") return CmdPending(args);
  if (cmd == "drain") return CmdDrain(args);
  std::cerr << "unknown command: " << cmd << "\n";
  return 1;
}
