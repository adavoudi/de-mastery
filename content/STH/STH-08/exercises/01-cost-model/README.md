# STH-08 / 01 — Cost model for three freshness tiers on one mart

Price the **same** revenue mart at three freshness tiers: `hours`, `minutes` and `seconds`. Use four
buckets: **compute**, **storage**, **operations** and the **total**. The point is the *shape*:
seconds > minutes > hours. Always-on compute and on-call drive this shape. The tests check your
numbers **within ±20%** of a reference model. So you only need plain deterministic arithmetic.

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

- `compute` captures the always-on cluster. The compaction work scales with the same always-on
  fraction. It is compute, not storage.
- `storage` is the retained bytes plus the small-file/snapshot overhead (`file_overhead`).
- `operations` is the people cost of monitoring and on-call.
- Return a dict with float values for `compute`, `storage`, `operations` and `total`.

## Constraints

- The function is deterministic and pure: no I/O, no randomness, no wall-clock.
- The three tier multipliers come from `TIERS`. The workload and the prices are identical across
  tiers (same data, different freshness).

## How the tests grade you (G1–G6)

- A **reference cost model** is the oracle. It is an independent implementation of the formulas
  above. Every component and the total must be **within ±20%** of it. This applies to all three
  tiers and to several workloads.
- The **ordering** must hold: `total(seconds) > total(minutes) > total(hours)`. The same ordering
  must hold for compute and operations.
- `total` must equal `compute + storage + operations` (no missing bucket).
- Storage must respond to `retention_days` and `file_overhead`. Compute must respond to the
  `always_on_fraction` of the tier (it must not be flat across tiers).
