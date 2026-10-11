#include "fair_share.h"

#include <algorithm>
#include <limits>

namespace datagen {

FairShareScheduler::FairShareScheduler(int num_tenants)
    : num_tenants_(num_tenants) {
  for (int t = 0; t < num_tenants_; ++t) {
    tenants_[t] = TenantState{};
  }
}

double FairShareScheduler::CostOf(int priority) const {
  // priority in [1,5]; cost in (0,1], higher priority -> smaller cost ->
  // virtual time climbs slower -> picked more often under contention.
  const int clamped = std::max(1, std::min(5, priority));
  return 1.0 / static_cast<double>(clamped);
}

void FairShareScheduler::NoteArrival(int tenant_id, int /*priority*/) {
  tenants_[tenant_id].admitted++;
}

void FairShareScheduler::NoteDispatch(int tenant_id, int priority) {
  auto& state = tenants_[tenant_id];
  state.virtual_time += CostOf(priority);
  state.dispatched++;
}

int FairShareScheduler::PickNextTenant(
    const std::unordered_map<int, int>& ready_tenant_counts) const {
  int best_tenant = -1;
  double best_vt = std::numeric_limits<double>::infinity();
  for (const auto& [tenant_id, count] : ready_tenant_counts) {
    if (count <= 0) continue;
    auto it = tenants_.find(tenant_id);
    const double vt = (it != tenants_.end()) ? it->second.virtual_time : 0.0;
    // Tie-break on the lower tenant_id so the result is deterministic
    // regardless of unordered_map iteration order (a real bug caught by
    // FairShare.PicksLowestVirtualTimeTenant: an earlier version picked
    // whichever tied tenant iteration happened to visit last).
    if (vt < best_vt || (vt == best_vt && tenant_id < best_tenant)) {
      best_vt = vt;
      best_tenant = tenant_id;
    }
  }
  return best_tenant;
}

double FairShareScheduler::VirtualTimeOf(int tenant_id) const {
  auto it = tenants_.find(tenant_id);
  return it != tenants_.end() ? it->second.virtual_time : 0.0;
}

}  // namespace datagen
