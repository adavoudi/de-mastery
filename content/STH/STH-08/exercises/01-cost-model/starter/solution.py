"""STH-08 / 01-cost-model — starter. See README.md.

Implement the deterministic cost model. The TIERS table is given; do not change
its multipliers. Return a dict with float compute/storage/operations/total.
"""

HOURS_PER_MONTH = 24 * 30  # 720

TIERS = {
    "hours":   {"always_on_fraction": 0.05, "file_overhead": 0.02, "ops_hours_month": 4.0},
    "minutes": {"always_on_fraction": 0.60, "file_overhead": 0.15, "ops_hours_month": 20.0},
    "seconds": {"always_on_fraction": 1.00, "file_overhead": 0.40, "ops_hours_month": 60.0},
}


def monthly_cost(tier, workload):
    # TODO: compute, storage, operations and total per the README formulas.
    #   compute     = always_on_fraction * 24 * 30 * compute_hour_usd
    #   retained_gb = events_per_day * retention_days * bytes_per_event / 1e9
    #   storage     = retained_gb * (1 + file_overhead) * storage_gb_month_usd
    #   operations  = ops_hours_month * ops_hour_usd
    #   total       = compute + storage + operations
    raise NotImplementedError
