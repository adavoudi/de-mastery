# STH-08 / 01 — Cost model for three freshness tiers on one mart

Price the **same** revenue mart at three freshness tiers — `hours`, `minutes`, `seconds` — in
four buckets: **compute**, **storage**, **operations**, and the **total**. The point is the
*shape*: seconds > minutes > hours, driven by always-on compute and on-call. Your numbers are
checked **within ±20%** of a reference model, so plain deterministic arithmetic is all you need.

## What you implement (`solution.py`)

```python
TIERS = {   # provided for you; do NOT change these multipliers
    "hours":   {"always_on_fraction": 0.05, "file_overhead": 0.02, "ops_hours_month": 4.0},
    "minutes": {"always_on_fraction": 0.60, "file_overhead": 0.15, "ops_hours_month": 20.0},
    "seconds": {"always_on_fraction": 1.00, "file_overhead": 0.40, "ops_hours_month": 60.0},
}

def monthly_cost(tier, workload): ...   # -> {"compute","storage","operations","total"}
```

`tier` is one of `"hours" | "minutes" | "seconds"`. `workload` is a dict:

| key | meaning |
|---|---|
| `events_per_day` | events ingested per day |
| `bytes_per_event` | average bytes per event (on the lake) |
| `retention_days` | days of data retained |
| `compute_hour_usd` | price of one always-on cluster unit per hour |
| `storage_gb_month_usd` | price per GB-month |
| `ops_hour_usd` | fully-loaded engineer cost per hour |

## The model (a 30-day month)

```
compute      = always_on_fraction * 24 * 30 * compute_hour_usd
retained_gb  = events_per_day * retention_days * bytes_per_event / 1e9
storage      = retained_gb * (1 + file_overhead) * storage_gb_month_usd
operations   = ops_hours_month * ops_hour_usd
total        = compute + storage + operations
```

- `compute` captures the always-on cluster (and the compaction work scales with the same
  always-on fraction — it is compute, not storage).
- `storage` is the retained bytes plus the small-file/snapshot overhead (`file_overhead`).
- `operations` is the monitoring / on-call people cost.
- Return a dict with float values for `compute`, `storage`, `operations`, `total`.

## Constraints

- Deterministic and pure: no I/O, no randomness, no wall-clock.
- The three tier multipliers come from `TIERS`; the workload and prices are identical across
  tiers (same data, different freshness).

## How you're graded (G1–G6)

- A **reference cost model** (independent implementation of the formulas above) is the oracle;
  every component and the total must be **within ±20%** of it, for all three tiers and across
  several workloads.
- The **ordering** must hold: `total(seconds) > total(minutes) > total(hours)` and the same for
  compute and operations.
- `total` must equal `compute + storage + operations` (no missing bucket).
- Storage must respond to `retention_days` and `file_overhead`; compute must respond to the
  tier's `always_on_fraction` (not be flat across tiers).
