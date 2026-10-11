#include <gtest/gtest.h>

#include <set>

#include "dag.h"

using datagen::ExpandDag;
using datagen::TemplateIndexFor;

TEST(Dag, DeterministicForSameRequestId) {
  auto a = ExpandDag("req-abc-123", "ingest_scan");
  auto b = ExpandDag("req-abc-123", "frame_mine");  // payload_kind not used
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    EXPECT_EQ(a[i].job_id, b[i].job_id);
    EXPECT_EQ(a[i].deps, b[i].deps);
  }
}

TEST(Dag, EveryDepIsAPriorJobInTheSameDag) {
  for (int i = 0; i < 50; ++i) {
    std::string rid = "req-sweep-" + std::to_string(i);
    auto jobs = ExpandDag(rid, "ingest_scan");
    std::set<std::string> seen;
    for (const auto& job : jobs) {
      for (const auto& dep : job.deps) {
        EXPECT_TRUE(seen.count(dep)) << "dep " << dep << " not defined before "
                                      << job.job_id << " in " << rid;
      }
      seen.insert(job.job_id);
    }
  }
}

TEST(Dag, ExactlyThreeTemplatesObserved) {
  std::set<int> templates;
  for (int i = 0; i < 300; ++i) {
    templates.insert(TemplateIndexFor("req-template-sweep-" + std::to_string(i)));
  }
  EXPECT_EQ(templates.size(), 3u);
}

TEST(Dag, LinearTemplateHasThreeJobsNoFanOut) {
  // Find a request_id that hits template 0 by brute search.
  for (int i = 0; i < 1000; ++i) {
    std::string rid = "req-find0-" + std::to_string(i);
    if (TemplateIndexFor(rid) == 0) {
      auto jobs = ExpandDag(rid, "x");
      ASSERT_EQ(jobs.size(), 3u);
      EXPECT_EQ(jobs[0].job_id, "ingest");
      EXPECT_EQ(jobs[2].job_id, "publish");
      return;
    }
  }
  FAIL() << "no request_id in range hit template 0";
}
