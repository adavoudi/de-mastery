"""STH-08 / 01-cost-model — alt solution: component registry (table-driven).

Genuinely different structure: each cost bucket is a named callable in a
registry, and `monthly_cost` folds over the registry and sums the results. Same
formulas, different shape.
"""

TIERS = {
    "hours":   {"always_on_fraction": 0.05, "file_overhead": 0.02, "ops_hours_month": 4.0},
    "minutes": {"always_on_fraction": 0.60, "file_overhead": 0.15, "ops_hours_month": 20.0},
    "seconds": {"always_on_fraction": 1.00, "file_overhead": 0.40, "ops_hours_month": 60.0},
}


def _retained_gb(w):
    return (w["events_per_day"] * w["retention_days"] * w["bytes_per_event"]) / 1_000_000_000


_COMPONENTS = {
    "compute": lambda t, w: t["always_on_fraction"] * (24 * 30) * w["compute_hour_usd"],
    "storage": lambda t, w: _retained_gb(w) * (1 + t["file_overhead"]) * w["storage_gb_month_usd"],
    "operations": lambda t, w: t["ops_hours_month"] * w["ops_hour_usd"],
}


def monthly_cost(tier, workload):
    t = TIERS[tier]
    out = {name: fn(t, workload) for name, fn in _COMPONENTS.items()}
    out["total"] = sum(out.values())
    return out
