"""C3 acceptance-test harness fixtures.

Selects which implementation to grade via the ``DE_C3_IMPL`` env var:

* ``starter``   (default) — the learner's incomplete code; tests FAIL/skip clearly.
* ``reference`` — the tiny proven impl; tests PASS (proves the harness is valid).

Also honours ``DE_SUBMISSION_DIR`` (set by ``de test``/``de validate``): if it
points at a dir containing ``jobs.py`` that module is loaded instead.

The streamhouse testkit fixtures (``paimon_sql``, ``iceberg_sql``, ``fluss_cluster``,
``serving_query``, ``sth_oracle``) are auto-registered by the de_testkit pytest
plugin — no imports needed. They SKIP cleanly when the native artifacts are
missing (run ``bash labs/fetch_streamhouse.sh``).
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
C3 = HERE.parent


def _load_jobs():
    sub = os.environ.get("DE_SUBMISSION_DIR")
    if sub:
        path = Path(sub) / "jobs.py"
    else:
        impl = os.environ.get("DE_C3_IMPL", "starter")
        path = C3 / impl / "jobs.py"
    if not path.exists():
        pytest.skip(f"C3 jobs module not found: {path}")
    spec = importlib.util.spec_from_file_location("c3_jobs", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def jobs():
    """The implementation module under test (starter or reference)."""
    return _load_jobs()


@pytest.fixture
def c3_build_dataset():
    """Return the ``build_c3_dataset`` callable, loaded by path (no package
    install needed)."""
    path = C3 / "data" / "generate.py"
    spec = importlib.util.spec_from_file_location("c3_generate", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod.build_c3_dataset


# --- tiny deterministic datasets (do not need de_data; keep the proof fast) ---
@pytest.fixture
def golden_sources() -> list[dict]:
    """Three partial rows for the SAME golden customer across companies.

    Each source provides different non-null fields; partial-update must merge
    them into one row keeping every non-null value.
    """
    return [
        {"match_key": "k1", "customer_id": 1, "name": "Alice",
         "email": None, "phone": None, "loyalty_tier": None},
        {"match_key": "k1", "customer_id": 1, "name": None,
         "email": "alice@x.com", "phone": None, "loyalty_tier": None},
        {"match_key": "k1", "customer_id": 1, "name": None,
         "email": None, "phone": "555-1", "loyalty_tier": "gold"},
    ]


@pytest.fixture
def golden_expected() -> list[list]:
    return [["k1", "Alice", "alice@x.com", "555-1", "gold"]]


@pytest.fixture
def fx_orders() -> dict:
    """Orders + a versioned FX table; the as-of rate differs from the latest."""
    orders = [
        {"order_id": 10, "currency": "EUR", "amount": 100.0, "order_date": "2024-01-10"},
        {"order_id": 11, "currency": "EUR", "amount": 100.0, "order_date": "2024-02-10"},
    ]
    fx_rates = [
        {"currency": "EUR", "rate_date": "2024-01-01", "usd_per_unit": 1.10},
        {"currency": "EUR", "rate_date": "2024-02-01", "usd_per_unit": 1.20},
    ]
    # as-of: order 10 -> 1.10 (usd 110), order 11 -> 1.20 (usd 120).
    expected = [
        [10, "EUR", 100.0, 1.10, 110.0],
        [11, "EUR", 100.0, 1.20, 120.0],
    ]
    return {"orders": orders, "fx_rates": fx_rates, "expected": expected}
