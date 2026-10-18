"""C3 — streaming == batch-rebuild acceptance tests (the G6 oracle at mart scale).

Each test grades the implementation module (starter -> FAIL/skip; reference ->
PASS). All reads are BATCH/bounded via the ``paimon_sql`` fixture; nothing
iterates an unbounded stream. The ``sth_oracle`` fixture asserts order-insensitive
multiset equality against an independent batch answer.

Milestones not yet reachable by the tiny proof (M1 SCD2 close/open, M4 full mart,
M6 Fluss tiering) are marked ``skip`` with a clear reason so they fail loudly only
once implemented — never silently pass.
"""
from __future__ import annotations

import pytest


@pytest.mark.timeout(120)
def test_m0_data_hook(tmp_path, c3_build_dataset):
    """M0 — the deterministic data hook runs and is reproducible."""
    a = c3_build_dataset(tmp_path / "a", seed=42, scale=0.001)
    b = c3_build_dataset(tmp_path / "b", seed=42, scale=0.001)
    for key in ("cdc_customers", "cdc_orders", "cdc_fx_rates"):
        assert a[key].read_text() == b[key].read_text(), f"{key} not deterministic"
    assert a["cdc_customers"].read_text().strip(), "empty customer changelog"


@pytest.mark.timeout(120)
def test_golden_record_partial_update(paimon_sql, sth_oracle, jobs,
                                      golden_sources, golden_expected):
    """M2 — partial-update golden record == expected merged row (G6 oracle).

    starter uses the wrong merge engine, so the last write wins and non-null
    fields are lost -> this FAILS for starter, PASSES for reference.
    """
    plan = jobs.golden_record(golden_sources)
    rows = paimon_sql.batch(ddl=plan["ddl"], inserts=plan["inserts"],
                            query=plan["query"])
    sth_oracle(rows, golden_expected)


@pytest.mark.timeout(120)
def test_fct_orders_fx_as_of(paimon_sql, sth_oracle, jobs, fx_orders):
    """M3 — fct_orders uses the FX rate AS OF order time (not the latest).

    starter raises NotImplementedError (M3 unimplemented) -> xfail-style FAIL;
    reference resolves the as-of rate correctly -> PASS.
    """
    plan = jobs.fct_orders_fx(fx_orders["orders"], fx_orders["fx_rates"])
    rows = paimon_sql.batch(ddl=plan["ddl"], inserts=plan["inserts"],
                            query=plan["query"])
    sth_oracle(rows, fx_orders["expected"])


@pytest.mark.timeout(120)
def test_scd2_current_rows():
    """M1 — SCD2 current-version rows == batch rebuild. Not in the tiny proof."""
    pytest.skip("M1 not implemented: build SCD2 dim_customer, then assert "
                "is_current rows == DuckDB batch rebuild via sth_oracle.")


@pytest.mark.timeout(300)
def test_oracle_full_mart():
    """M4 — the full streaming mart == batch rebuild at capstone scale."""
    pytest.skip("M4 not implemented: wire M1+M2+M3 over the de_data CDC changelog "
                "and compare the whole mart to a DuckDB rebuild via sth_oracle.")


@pytest.mark.timeout(300)
def test_fluss_lake_roundtrip():
    """M6 — Fluss upsert tiered to Paimon reads identically in batch. Opt-in."""
    pytest.skip("M6 not implemented: fluss_cluster(paimon_warehouse=wh), write "
                "upsert, read `t$lake` in batch == streaming read (STH-03).")
