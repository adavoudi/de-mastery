"""STH-08 / 01-cost-model — alt solution: dataclass + per-day build-up.

Genuinely different again: a dataclass holds the tier multipliers, storage is
built up per retained day (sum over days) rather than multiplied in one shot, and
compute is derived from a monthly minutes figure. Arithmetically equivalent to
the reference within rounding; comfortably inside the +-20% tolerance.
"""

from dataclasses import dataclass

_MULTS = {
    "hours":   (0.05, 0.02, 4.0),
    "minutes": (0.60, 0.15, 20.0),
    "seconds": (1.00, 0.40, 60.0),
}


@dataclass
class _Tier:
    always_on_fraction: float
    file_overhead: float
    ops_hours_month: float


def monthly_cost(tier, workload):
    t = _Tier(*_MULTS[tier])
    # compute via always-on minutes/month -> hours.
    minutes_month = t.always_on_fraction * 60 * 24 * 30
    compute = (minutes_month / 60.0) * workload["compute_hour_usd"]
    # storage summed day-by-day over the retention window.
    gb_per_day = (workload["events_per_day"] * workload["bytes_per_event"]) / 1e9
    retained_gb = sum(gb_per_day for _ in range(int(workload["retention_days"])))
    storage = retained_gb * (1 + t.file_overhead) * workload["storage_gb_month_usd"]
    operations = t.ops_hours_month * workload["ops_hour_usd"]
    return {
        "compute": compute,
        "storage": storage,
        "operations": operations,
        "total": compute + storage + operations,
    }
