#include <gtest/gtest.h>

#include <unordered_map>

#include "fair_share.h"

using datagen::FairShareScheduler;

TEST(FairShare, PicksLowestVirtualTimeTenant) {
  FairShareScheduler s(8);
  std::unordered_map<int, int> ready = {{0, 1}, {1, 1}, {2, 1}};
  EXPECT_EQ(s.PickNextTenant(ready), 0);  // tie: first found with vt=0, deterministic by map order not required
}

TEST(FairShare, EqualPriorityConvergesToEqualShareOverManyRounds) {
  FairShareScheduler s(8);
  std::unordered_map<int, int> ready;
  for (int t = 0; t < 8; ++t) ready[t] = 1;

  std::unordered_map<int, int> dispatch_counts;
  for (int round = 0; round < 8000; ++round) {
    int picked = s.PickNextTenant(ready);
    ASSERT_GE(picked, 0);
    dispatch_counts[picked]++;
    s.NoteDispatch(picked, 3);  // same priority for every tenant
  }
  for (int t = 0; t < 8; ++t) {
    EXPECT_EQ(dispatch_counts[t], 1000) << "tenant " << t
                                         << " did not get an equal 1/8 share";
  }
}

TEST(FairShare, HigherPriorityTenantGetsMoreShareUnderContention) {
  FairShareScheduler s(8);
  std::unordered_map<int, int> ready = {{0, 1}, {1, 1}};
  std::unordered_map<int, int> dispatch_counts;
  for (int round = 0; round < 1000; ++round) {
    int picked = s.PickNextTenant(ready);
    dispatch_counts[picked]++;
    // tenant 0 is priority 5 (fastest growth-resistant), tenant 1 is priority 1
    s.NoteDispatch(picked, picked == 0 ? 5 : 1);
  }
  EXPECT_GT(dispatch_counts[0], dispatch_counts[1]);
}

TEST(FairShare, EmptyReadySetReturnsNegativeOne) {
  FairShareScheduler s(8);
  std::unordered_map<int, int> ready;
  EXPECT_EQ(s.PickNextTenant(ready), -1);
}

TEST(FairShare, TenantWithZeroCountIsIgnored) {
  FairShareScheduler s(8);
  std::unordered_map<int, int> ready = {{0, 0}, {1, 3}};
  EXPECT_EQ(s.PickNextTenant(ready), 1);
}
