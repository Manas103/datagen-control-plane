#pragma once

#include <cstdint>
#include <optional>
#include <unordered_map>

namespace datagen {

// Max-min fair share dispatch order across a fixed tenant set, with
// priority (1..5) acting as a weight: a higher-priority tenant's virtual
// time advances more slowly, so it is picked more often relative to a
// lower-priority tenant submitting at the same rate. This is the classic
// weighted virtual-time / deficit-style fair queue, applied to *dispatch
// order* (which tenant's next ready job runs next), not to admission
// (admission always succeeds in this design; see README).
class FairShareScheduler {
 public:
  explicit FairShareScheduler(int num_tenants);

  // Called once per admitted request to register demand; cheap, O(1).
  void NoteArrival(int tenant_id, int priority);

  // Called by the dispatcher when it successfully dispatches one job
  // belonging to this tenant; advances the tenant's virtual time.
  void NoteDispatch(int tenant_id, int priority);

  // Among the tenants in `ready_tenants` (tenants that currently have at
  // least one ready job), returns the one with the lowest virtual time,
  // i.e. the one most entitled to run next. Returns -1 if the set is empty.
  int PickNextTenant(const std::unordered_map<int, int>& ready_tenant_counts) const;

  double VirtualTimeOf(int tenant_id) const;

 private:
  struct TenantState {
    double virtual_time = 0.0;
    int64_t admitted = 0;
    int64_t dispatched = 0;
  };

  double CostOf(int priority) const;

  int num_tenants_;
  std::unordered_map<int, TenantState> tenants_;
};

}  // namespace datagen
