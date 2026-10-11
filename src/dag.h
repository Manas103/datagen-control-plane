#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace datagen {

struct JobSpec {
  std::string job_id;
  std::vector<std::string> deps; // job_ids within the same request
};

// Deterministic DAG expansion: a pure function of (request_id, payload_kind).
// The same request_id always expands to the exact same job set and
// dependency edges, which is what makes the replay digest test meaningful:
// resubmitting a duplicate request_id must not produce a different DAG.
std::vector<JobSpec> ExpandDag(const std::string& request_id,
                                const std::string& payload_kind);

// Picks one of three fixed DAG templates by hashing request_id. Exposed
// separately so tests can assert on template selection directly.
int TemplateIndexFor(const std::string& request_id);

}  // namespace datagen
