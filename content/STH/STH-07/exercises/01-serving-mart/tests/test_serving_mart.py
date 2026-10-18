"""Tests for STH-07 / 01-serving-mart.

The serving layer answers a sub-second revenue dashboard off a PRE-AGGREGATED
rollup of the merged streaming mart. Grading, by gate:

* G6 oracle: an INDEPENDENT DuckDB aggregate over the same merged rows is the
  source of truth; the learner's dashboard must equal it for every company.
* pre-aggregation (contract: "pruning/plan OR pre-aggregation"): `mart_daily`
  must exist and be strictly smaller than the raw fact, AND the dashboard must
  still answer after the raw `fct_orders` is DROPPED — proving it reads the
  rollup, not the raw fact (kills the no-preagg mutant robustly, no plan parsing).
* robust latency guardrail: warm-up, then the MEDIAN of several runs must beat a
  generous budget — safe on a loaded machine; never the proof on its own.

Most of the suite runs on a bare machine (DuckDB is in the main venv), so G1 is
always reachable. A final streamhouse-backed test builds the mart in Paimon and
reads the merged state via the pure-Python serving fixture; it SKIPs cleanly when
the vendored artifacts are absent.
"""

from __future__ import annotations

import statistics
import time

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Deterministic merged mart (what the serving layer reads: latest per order_id).
# Two companies, several days; revenue in minor units. Chosen so per-day sums
# differ across companies and the DESC ordering is unambiguous.
# ---------------------------------------------------------------------------
def _merged_rows():
    rows = []
    oid = 0
    plan = {
        "ACME": {"2024-01-01": [100, 250, 90], "2024-01-02": [400],
                 "2024-01-03": [50, 50, 50, 50]},
        "GLOBEX": {"2024-01-01": [700], "2024-01-02": [120, 130],
                   "2024-01-03": [999]},
    }
    for company, days in plan.items():
        for day, amounts in days.items():
            for amt in amounts:
                oid += 1
                rows.append({"order_id": oid, "company": company,
                             "order_day": day, "revenue": amt})
    return rows


ROWS = _merged_rows()
COMPANIES = ["ACME", "GLOBEX"]


def _oracle(company):
    """Independent DuckDB aggregate over the merged rows (G6 source of truth)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE raw(company VARCHAR, order_day VARCHAR, revenue BIGINT)")
    con.executemany("INSERT INTO raw VALUES (?,?,?)",
                    [(r["company"], r["order_day"], r["revenue"]) for r in ROWS])
    out = con.execute(
        "SELECT order_day, SUM(revenue) FROM raw WHERE company = ? "
        "GROUP BY order_day ORDER BY SUM(revenue) DESC, order_day ASC",
        [company]).fetchall()
    con.close()
    return [(d, int(v)) for d, v in out]


def _fresh(submission):
    con = duckdb.connect()
    submission.load_serving(con, ROWS)
    return con


# ---------------------------------------------------------------------------
# G6 oracle: dashboard == independent recompute, for every company
# ---------------------------------------------------------------------------
def test_dashboard_matches_oracle_per_company(submission):
    con = _fresh(submission)
    for company in COMPANIES:
        assert submission.dashboard_query(con, company) == _oracle(company), company
    con.close()


def test_dashboard_equals_olap_scan(submission):
    # rollup-backed dashboard must equal the raw OLAP recompute (same answer,
    # two code paths) — a second independent cross-check.
    con = _fresh(submission)
    for company in COMPANIES:
        assert submission.dashboard_query(con, company) == submission.olap_scan(con, company), company
    con.close()


def test_ordering_is_revenue_desc_then_day(submission):
    con = _fresh(submission)
    res = submission.dashboard_query(con, "ACME")
    revenues = [v for _, v in res]
    assert revenues == sorted(revenues, reverse=True), res
    # tie-break: equal revenues ordered by day ASC
    for i in range(len(res) - 1):
        if res[i][1] == res[i + 1][1]:
            assert res[i][0] <= res[i + 1][0], res
    con.close()


# ---------------------------------------------------------------------------
# pre-aggregation: mart_daily exists, is smaller, and the dashboard survives
# dropping the raw fact (so it cannot be scanning fct_orders)
# ---------------------------------------------------------------------------
def test_mart_daily_is_pre_aggregated(submission):
    con = _fresh(submission)
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "mart_daily" in tables, f"no pre-aggregated rollup table; tables={tables}"
    n_fact = con.execute("SELECT count(*) FROM fct_orders").fetchone()[0]
    n_mart = con.execute("SELECT count(*) FROM mart_daily").fetchone()[0]
    assert n_mart < n_fact, (
        f"mart_daily ({n_mart}) must be smaller than fct_orders ({n_fact})")
    # one row per (company, order_day)
    distinct = con.execute(
        "SELECT count(*) FROM (SELECT DISTINCT company, order_day FROM fct_orders)"
    ).fetchone()[0]
    assert n_mart == distinct, (n_mart, distinct)
    con.close()


def test_dashboard_reads_rollup_not_raw_fact(submission):
    # Drop the raw fact AFTER load_serving. A correct dashboard reads mart_daily
    # and still works; a dashboard that scans fct_orders now errors or returns
    # nothing. Robust, no plan-string parsing.
    con = _fresh(submission)
    expected = {c: submission.dashboard_query(con, c) for c in COMPANIES}
    con.execute("DROP TABLE fct_orders")
    for company in COMPANIES:
        assert submission.dashboard_query(con, company) == expected[company], company
    con.close()


# ---------------------------------------------------------------------------
# robust latency guardrail (generous; median of N; warm-up first)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(120)
def test_dashboard_latency_under_budget(submission):
    con = _fresh(submission)
    # warm-up (JIT, caches, catalog) — not measured.
    for _ in range(3):
        submission.dashboard_query(con, "ACME")
    samples = []
    for _ in range(9):
        t0 = time.perf_counter()
        submission.dashboard_query(con, "ACME")
        samples.append(time.perf_counter() - t0)
    median = statistics.median(samples)
    # VERY generous budget: a rollup read of a few rows is sub-millisecond; 0.5 s
    # median leaves huge slack for a loaded 32 GB box running 4 agents.
    assert median < 0.5, f"median dashboard latency {median*1000:.1f} ms too high"
    con.close()


# ---------------------------------------------------------------------------
# streamhouse-backed layer: build the mart in Paimon, read merged via serving.
# SKIPs cleanly when the vendored artifacts are missing (bare machine stays green).
# ---------------------------------------------------------------------------
@pytest.mark.timeout(300)
def test_serving_reads_merged_paimon_state(submission, paimon_sql, serving_query):
    # Partial-update PK fact: a late event fills revenue for order 1; serving must
    # read the MERGED latest-per-key state (not the changelog).
    paimon_sql.batch(
        ddl=["CREATE TABLE fct (order_id INT, company STRING, order_day STRING, "
             "revenue INT, PRIMARY KEY(order_id) NOT ENFORCED) WITH "
             "('merge-engine'='partial-update','bucket'='1')"],
        inserts=[
            "INSERT INTO fct VALUES (1,'ACME','2024-01-01',CAST(NULL AS INT))",
            "INSERT INTO fct VALUES (1,CAST(NULL AS STRING),CAST(NULL AS STRING),500)",
            "INSERT INTO fct VALUES (2,'ACME','2024-01-01',300)",
            "INSERT INTO fct VALUES (3,'GLOBEX','2024-01-02',700)",
        ])
    served = serving_query.paimon(str(paimon_sql.warehouse), "default.fct")
    rows = [{"order_id": int(r["order_id"]), "company": r["company"],
             "order_day": r["order_day"], "revenue": int(r["revenue"])}
            for r in served]
    con = duckdb.connect()
    submission.load_serving(con, rows)
    # order 1 merged to revenue=500 -> ACME 2024-01-01 = 500+300 = 800.
    res = dict(submission.dashboard_query(con, "ACME"))
    assert res["2024-01-01"] == 800, served
    con.close()
