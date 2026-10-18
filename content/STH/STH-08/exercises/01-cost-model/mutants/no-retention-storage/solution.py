"""MUTANT no-retention-storage — storage ignores retention_days (one day only)."""

HOURS_PER_MONTH = 24 * 30

TIERS = {
    "hours":   {"always_on_fraction": 0.05, "file_overhead": 0.02, "ops_hours_month": 4.0},
    "minutes": {"always_on_fraction": 0.60, "file_overhead": 0.15, "ops_hours_month": 20.0},
    "seconds": {"always_on_fraction": 1.00, "file_overhead": 0.40, "ops_hours_month": 60.0},
}


def monthly_cost(tier, workload):
    t = TIERS[tier]
    compute = t["always_on_fraction"] * HOURS_PER_MONTH * workload["compute_hour_usd"]
    # BUG: retention_days dropped — only a single day of data is priced.
    retained_gb = (workload["events_per_day"] * workload["bytes_per_event"]) / 1e9
    storage = retained_gb * (1.0 + t["file_overhead"]) * workload["storage_gb_month_usd"]
    operations = t["ops_hours_month"] * workload["ops_hour_usd"]
    return {"compute": compute, "storage": storage, "operations": operations,
            "total": compute + storage + operations}
