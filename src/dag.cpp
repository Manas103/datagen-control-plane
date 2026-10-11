#include "dag.h"

#include <functional>

namespace datagen {

int TemplateIndexFor(const std::string& request_id) {
  // std::hash is not guaranteed stable across process runs for all types in
  // general, but libstdc++'s std::hash<std::string> (the only standard
  // library this project builds against) is a pure FNV-1a-derived function
  // of the bytes with no per-process seed, so it is stable here. Documented
  // as a known portability caveat in the README rather than relied on
  // silently.
  size_t h = std::hash<std::string>{}(request_id);
  return static_cast<int>(h % 3);
}

std::vector<JobSpec> ExpandDag(const std::string& request_id,
                                const std::string& /*payload_kind*/) {
  const int tmpl = TemplateIndexFor(request_id);
  std::vector<JobSpec> jobs;

  if (tmpl == 0) {
    // Linear chain: ingest -> transform -> publish
    jobs.push_back({"ingest", {}});
    jobs.push_back({"transform", {"ingest"}});
    jobs.push_back({"publish", {"transform"}});
  } else if (tmpl == 1) {
    // Fan-out / fan-in: ingest -> {shard_a, shard_b, shard_c} -> publish
    jobs.push_back({"ingest", {}});
    jobs.push_back({"shard_a", {"ingest"}});
    jobs.push_back({"shard_b", {"ingest"}});
    jobs.push_back({"shard_c", {"ingest"}});
    jobs.push_back({"publish", {"shard_a", "shard_b", "shard_c"}});
  } else {
    // Diamond: ingest -> {validate, enrich} -> merge -> publish
    jobs.push_back({"ingest", {}});
    jobs.push_back({"validate", {"ingest"}});
    jobs.push_back({"enrich", {"ingest"}});
    jobs.push_back({"merge", {"validate", "enrich"}});
    jobs.push_back({"publish", {"merge"}});
  }
  return jobs;
}

}  // namespace datagen
