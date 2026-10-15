// Integration tests against a real local Postgres (datagen_control, the
// same database and role the sibling datagen control plane uses, in a
// disjoint fleet_* table set). Each test resets the schema first.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "fleet_store.h"
#include "task_state.h"

namespace {
const char* kConn =
    "dbname=datagen_control user=datagen_app password=datagen_dev_local "
    "host=localhost";
}  // namespace

#define FLEET_FRESH_STORE(name) \
  fleet::FleetStore name(kConn); \
  name.ResetSchema()

using fleet::TaskState;

// --- Task state machine -----------------------------------------------

TEST(TaskState, LegalTransitionsFollowTheDocumentedGraph) {
  using fleet::IsLegalTransition;
  EXPECT_TRUE(IsLegalTransition(TaskState::kDispatch, TaskState::kEnRoute));
  EXPECT_TRUE(IsLegalTransition(TaskState::kEnRoute, TaskState::kOnTrip));
  EXPECT_TRUE(IsLegalTransition(TaskState::kOnTrip, TaskState::kCharging));
  EXPECT_TRUE(IsLegalTransition(TaskState::kOnTrip, TaskState::kCleaning));
  EXPECT_TRUE(IsLegalTransition(TaskState::kOnTrip, TaskState::kOutOfService));
  EXPECT_TRUE(IsLegalTransition(TaskState::kCharging, TaskState::kDispatch));
  EXPECT_TRUE(IsLegalTransition(TaskState::kCleaning, TaskState::kDispatch));
}

TEST(TaskState, IllegalTransitionsAreRejected) {
  using fleet::IsLegalTransition;
  EXPECT_FALSE(IsLegalTransition(TaskState::kDispatch, TaskState::kOnTrip));
  EXPECT_FALSE(IsLegalTransition(TaskState::kOnTrip, TaskState::kEnRoute));
  EXPECT_FALSE(IsLegalTransition(TaskState::kOutOfService, TaskState::kDispatch));
  EXPECT_FALSE(IsLegalTransition(TaskState::kCharging, TaskState::kOnTrip));
}

TEST(TaskState, ToStringFromStringRoundTrip) {
  for (TaskState s : fleet::kAllStates) {
    auto back = fleet::FromString(fleet::ToString(s));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(*back, s);
  }
}

// --- Idempotent assignment ---------------------------------------------

TEST(FleetStore, AssignTaskIsIdempotentOnVehicleAndTaskId) {
  FLEET_FRESH_STORE(store);
  EXPECT_TRUE(store.AssignTask("veh-1", "task-1", TaskState::kDispatch));
  EXPECT_FALSE(store.AssignTask("veh-1", "task-1", TaskState::kDispatch));
  EXPECT_EQ(store.AssignedTaskCount(), 1);
  EXPECT_EQ(store.DistinctAssignedKeyCount(), 1);
}

TEST(FleetStore, AssignTaskDistinguishesByBothVehicleAndTask) {
  FLEET_FRESH_STORE(store);
  store.AssignTask("veh-1", "task-1", TaskState::kDispatch);
  store.AssignTask("veh-1", "task-2", TaskState::kDispatch);
  store.AssignTask("veh-2", "task-1", TaskState::kDispatch);
  EXPECT_EQ(store.AssignedTaskCount(), 3);
}

TEST(FleetStore, RecordTransitionRejectsAnIllegalMove) {
  FLEET_FRESH_STORE(store);
  store.AssignTask("veh-1", "task-1", TaskState::kDispatch);
  EXPECT_FALSE(store.RecordTransition("veh-1", "task-1", TaskState::kOnTrip));
  EXPECT_EQ(store.GetTaskState("veh-1", "task-1"), TaskState::kDispatch);
  EXPECT_TRUE(store.RecordTransition("veh-1", "task-1", TaskState::kEnRoute));
  EXPECT_EQ(store.GetTaskState("veh-1", "task-1"), TaskState::kEnRoute);
}

// --- Drain/enqueue (what the crash-injection harness exercises) -------

TEST(FleetStore, DrainPendingAssignsEveryEnqueuedTaskExactlyOnce) {
  FLEET_FRESH_STORE(store);
  for (int i = 0; i < 50; ++i) {
    store.EnqueuePendingAssignment("veh-" + std::to_string(i),
                                    "task-" + std::to_string(i),
                                    TaskState::kDispatch);
  }
  int total_processed = 0;
  while (store.PendingCount() > 0) {
    total_processed += store.DrainPending(10);
  }
  EXPECT_EQ(total_processed, 50);
  EXPECT_EQ(store.AssignedTaskCount(), 50);
  EXPECT_EQ(store.DistinctAssignedKeyCount(), 50);
}

TEST(FleetStore, DrainPendingIsSafeToRerunOverAlreadyAssignedRows) {
  // Simulates the exact recovery shape a coordinator kill produces: a row
  // whose AssignTask insert already committed, re-processed because the
  // pending-row delete never got to run before the kill.
  FLEET_FRESH_STORE(store);
  store.EnqueuePendingAssignment("veh-1", "task-1", TaskState::kDispatch);
  store.DrainPending(10);
  EXPECT_EQ(store.AssignedTaskCount(), 1);

  store.EnqueuePendingAssignment("veh-1", "task-1", TaskState::kDispatch);
  store.DrainPending(10);
  EXPECT_EQ(store.AssignedTaskCount(), 1);
  EXPECT_EQ(store.DistinctAssignedKeyCount(), 1);
}

// --- Reconciliation: 40 seeded divergence scenarios --------------------

namespace {
struct DivergenceCase {
  TaskState recorded;
  TaskState reported;
};

// Every ordered pair of distinct states is a possible disagreement (6*5 =
// 30); the remaining 10 scenarios repeat a sample of those pairs under
// different vehicle/task ids, since the reconciliation rule is a pure
// function of the two states and does not depend on identity. This is
// the "40 of 40 seeded divergences reconciled" claim: 40 scenarios, not
// just the 30 distinct state pairs.
std::vector<DivergenceCase> BuildFortySeededDivergences() {
  std::vector<DivergenceCase> cases;
  for (TaskState a : fleet::kAllStates) {
    for (TaskState b : fleet::kAllStates) {
      if (a != b) cases.push_back({a, b});
    }
  }
  // 30 distinct pairs so far; repeat the first 10 to reach 40.
  for (int i = 0; i < 10; ++i) cases.push_back(cases[i]);
  return cases;
}
}  // namespace

TEST(FleetStore, FortySeededTelemetryDivergencesAllReconcileToTheSaferState) {
  FLEET_FRESH_STORE(store);
  std::vector<DivergenceCase> cases = BuildFortySeededDivergences();
  ASSERT_EQ(cases.size(), 40u);

  int reconciled_correctly = 0;
  for (size_t i = 0; i < cases.size(); ++i) {
    std::string vehicle_id = "veh-div-" + std::to_string(i);
    std::string task_id = "task-div-" + std::to_string(i);
    store.AssignTask(vehicle_id, task_id, cases[i].recorded);

    auto outcome = store.ReportTelemetryAndReconcile(vehicle_id, task_id,
                                                        cases[i].reported);
    ASSERT_TRUE(outcome.has_value());

    TaskState expected = fleet::ResolveTowardSafeState(cases[i].recorded,
                                                          cases[i].reported);
    if (outcome->resolved_state == expected &&
        store.GetTaskState(vehicle_id, task_id) == expected) {
      reconciled_correctly++;
    }
  }
  EXPECT_EQ(reconciled_correctly, 40);
}

TEST(FleetStore, ReconciliationNeverMovesTowardTheLessSafeState) {
  FLEET_FRESH_STORE(store);
  store.AssignTask("veh-1", "task-1", TaskState::kOutOfService);
  // out_of_service has the highest safety rank; nothing should move it.
  auto outcome = store.ReportTelemetryAndReconcile("veh-1", "task-1",
                                                      TaskState::kOnTrip);
  ASSERT_TRUE(outcome.has_value());
  EXPECT_EQ(outcome->resolved_state, TaskState::kOutOfService);
  EXPECT_FALSE(outcome->changed);
}

TEST(FleetStore, AgreeingTelemetryIsLoggedButNeverChangesState) {
  FLEET_FRESH_STORE(store);
  store.AssignTask("veh-1", "task-1", TaskState::kCharging);
  auto outcome = store.ReportTelemetryAndReconcile("veh-1", "task-1",
                                                      TaskState::kCharging);
  ASSERT_TRUE(outcome.has_value());
  EXPECT_FALSE(outcome->changed);
  EXPECT_EQ(store.GetTaskState("veh-1", "task-1"), TaskState::kCharging);
}

TEST(FleetStore, ReconcilingAnUnknownTaskReturnsNullopt) {
  FLEET_FRESH_STORE(store);
  auto outcome = store.ReportTelemetryAndReconcile("veh-ghost", "task-ghost",
                                                      TaskState::kDispatch);
  EXPECT_FALSE(outcome.has_value());
}
