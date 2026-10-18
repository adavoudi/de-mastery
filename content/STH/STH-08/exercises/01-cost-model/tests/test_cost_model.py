"""Tests for STH-08 / 01-cost-model.

Pure-Python, deterministic, no JVM / Docker — this whole suite runs on a bare
machine, so G1 is always reachable.

Grading, by gate:

* G6 oracle: an INDEPENDENT reference implementation of the documented formulas
  is the source of truth. Every component (compute, storage, operations) and the
  total must land within +-20% of the reference, for all three tiers across
  several workloads.
* invariant: the ordering seconds > minutes > hours must hold for total, compute
  and operations (the whole point of the module).
* structure: total == compute + storage + operations; storage must respond to
  retention_days; compute must respond to the tier (not be flat).

The +-20% band is wide enough for honestly-different implementations (G3 alts
compute the same formulas via a registry and via a day-by-day build-up) but far
too tight for the designed mutants (inverted tiers, dropped bucket, flat compute,
dropped retention).
"""

from __future__ import annotations

import pytest

TIERS = ["hours", "minutes", "seconds"]
TOL = 0.20  # +-20%

# Workloads chosen so operations is a MATERIAL share of the seconds-tier total
# (kills "missing ops") and retention_days >= 2 (kills "no-retention storage").
WORKLOADS = [
    {"events_per_day": 50_000_000, "bytes_per_event": 400, "retention_days": 30,
     "compute_hour_usd": 2.0, "storage_gb_month_usd": 0.023, "ops_hour_usd": 100.0},
    {"events_per_day": 5_000_000, "bytes_per_event": 1_200, "retention_days": 7,
     "compute_hour_usd": 3.5, "storage_gb_month_usd": 0.021, "ops_hour_usd": 120.0},
    {"events_per_day": 200_000_000, "bytes_per_event": 250, "retention_days": 14,
     "compute_hour_usd": 1.5, "storage_gb_month_usd": 0.023, "ops_hour_usd": 90.0},
]

# Independent reference model (the G6 oracle). Same formulas as the README,
# written here so the expected values do not come from the learner's code.
_REF_TIERS = {
    "hours":   {"always_on_fraction": 0.05, "file_overhead": 0.02, "ops_hours_month": 4.0},
    "minutes": {"always_on_fraction": 0.60, "file_overhead": 0.15, "ops_hours_month": 20.0},
    "seconds": {"always_on_fraction": 1.00, "file_overhead": 0.40, "ops_hours_month": 60.0},
}


def _reference(tier, w):
    t = _REF_TIERS[tier]
    compute = t["always_on_fraction"] * 24 * 30 * w["compute_hour_usd"]
    retained_gb = w["events_per_day"] * w["retention_days"] * w["bytes_per_event"] / 1e9
    storage = retained_gb * (1 + t["file_overhead"]) * w["storage_gb_month_usd"]
    operations = t["ops_hours_month"] * w["ops_hour_usd"]
    return {"compute": compute, "storage": storage, "operations": operations,
            "total": compute + storage + operations}


def _within(got, ref, tol=TOL):
    # relative error vs reference; guard the (never-zero here) ref==0 case.
    if ref == 0:
        return abs(got) <= 1e-9
    return abs(got - ref) / abs(ref) <= tol


# ---------------------------------------------------------------------------
# G6: within +-20% of the reference, every component + total, every tier/workload
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("wi", range(len(WORKLOADS)))
@pytest.mark.parametrize("tier", TIERS)
def test_within_tolerance_of_reference(submission, tier, wi):
    w = WORKLOADS[wi]
    got = submission.monthly_cost(tier, w)
    ref = _reference(tier, w)
    for key in ("compute", "storage", "operations", "total"):
        assert _within(got[key], ref[key]), (
            f"{tier} {key}: got {got[key]:.2f} vs ref {ref[key]:.2f} "
            f"(>{int(TOL*100)}%)")


# ---------------------------------------------------------------------------
# invariant: seconds > minutes > hours (total, compute, operations)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("wi", range(len(WORKLOADS)))
def test_tier_ordering(submission, wi):
    w = WORKLOADS[wi]
    costs = {tier: submission.monthly_cost(tier, w) for tier in TIERS}
    for key in ("total", "compute", "operations"):
        h, m, s = costs["hours"][key], costs["minutes"][key], costs["seconds"][key]
        assert s > m > h, f"{key} ordering not seconds>minutes>hours: {h=} {m=} {s=}"


# ---------------------------------------------------------------------------
# structure: total is the sum of the three buckets
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tier", TIERS)
def test_total_is_sum_of_buckets(submission, tier):
    w = WORKLOADS[0]
    got = submission.monthly_cost(tier, w)
    assert got["total"] == pytest.approx(
        got["compute"] + got["storage"] + got["operations"], rel=1e-9), got


# ---------------------------------------------------------------------------
# sensitivity: storage responds to retention; compute responds to the tier
# ---------------------------------------------------------------------------
def test_storage_scales_with_retention(submission):
    base = dict(WORKLOADS[0]); base["retention_days"] = 10
    more = dict(WORKLOADS[0]); more["retention_days"] = 20
    s_base = submission.monthly_cost("seconds", base)["storage"]
    s_more = submission.monthly_cost("seconds", more)["storage"]
    # doubling retention should ~double storage (not flat, not a tiny bump).
    assert s_more > 1.8 * s_base, (s_base, s_more)


def test_compute_responds_to_tier(submission):
    w = WORKLOADS[0]
    c_hours = submission.monthly_cost("hours", w)["compute"]
    c_seconds = submission.monthly_cost("seconds", w)["compute"]
    # seconds is always-on vs batch's small fraction: clearly, not flat.
    assert c_seconds > 2.0 * c_hours, (c_hours, c_seconds)


# ---------------------------------------------------------------------------
# operations is a material share of the seconds total (so dropping it is caught)
# ---------------------------------------------------------------------------
def test_operations_is_material_in_seconds_tier(submission):
    w = WORKLOADS[0]
    got = submission.monthly_cost("seconds", w)
    assert got["operations"] > TOL * got["total"], got
