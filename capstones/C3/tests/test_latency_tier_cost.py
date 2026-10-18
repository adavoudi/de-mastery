"""C3 / M7 — latency-tier cost model acceptance test.

Validates the cost template is (a) self-consistent — total == compute+storage+ops
within ±20% per tier — and (b) monotonic: faster freshness costs at least as much
(seconds >= minutes >= hours). This grades the learner's EDITED numbers; the
shipped template already satisfies it so the harness is demonstrably valid.
"""
from __future__ import annotations

from pathlib import Path

import pytest

try:
    import yaml  # PyYAML ships with the project
except Exception:  # pragma: no cover
    yaml = None

COST = Path(__file__).resolve().parent.parent / "cost" / "latency_tiers.yaml"
TOL = 0.20
ORDER = ["seconds", "minutes", "hours"]


@pytest.mark.timeout(30)
def test_latency_tier_cost():
    if yaml is None:
        pytest.skip("PyYAML not available")
    assert COST.exists(), f"missing cost template: {COST}"
    tiers = yaml.safe_load(COST.read_text())["tiers"]

    totals = []
    for name in ORDER:
        t = tiers[name]
        parts = t["compute_usd_month"] + t["storage_usd_month"] + t["ops_usd_month"]
        total = t["total_usd_month"]
        assert abs(total - parts) <= TOL * parts, (
            f"tier {name}: total {total} not within ±20% of "
            f"compute+storage+ops {parts}")
        totals.append(total)

    # Monotonic: seconds >= minutes >= hours (freshness costs money).
    assert totals[0] >= totals[1] >= totals[2], (
        f"cost not monotonic with freshness: {dict(zip(ORDER, totals))}")
