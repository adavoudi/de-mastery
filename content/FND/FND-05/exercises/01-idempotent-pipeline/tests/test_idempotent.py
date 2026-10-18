"""Tests for FND-05/01-idempotent-pipeline.

Oracle (G6): DuckDB computes the deduped revenue rollup independently over the real
commerce order_items (which contains deliberate duplicate events). Hypothesis checks
the idempotency and order-independence invariants.
"""

from __future__ import annotations

import copy

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


def _load_order_items(duckdb_con, parquet_path):
    """Load order_items as a list of dicts (stable types via DuckDB)."""
    rows = duckdb_con.execute(
        "SELECT company_id, order_item_id, order_id, product_code, qty, line_amount "
        "FROM read_parquet(?)",
        [str(parquet_path)],
    ).fetchall()
    cols = ["company_id", "order_item_id", "order_id", "product_code", "qty",
            "line_amount"]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# dedup vs the DuckDB oracle over the real (dirty) data
# ---------------------------------------------------------------------------
def test_dedup_count_matches_oracle(submission, duckdb_con, commerce_data):
    pq = commerce_data["parquet"]["order_items"]
    events = _load_order_items(duckdb_con, pq)
    out = submission.dedup(events, ["company_id", "order_item_id"])
    expected = duckdb_con.execute(
        "SELECT count(*) FROM (SELECT DISTINCT company_id, order_item_id "
        "FROM read_parquet(?))",
        [str(pq)],
    ).fetchone()[0]
    assert len(out) == expected
    # Kept keys are themselves unique.
    keys = [(r["company_id"], r["order_item_id"]) for r in out]
    assert len(keys) == len(set(keys))


def test_revenue_matches_oracle(submission, duckdb_con, commerce_data):
    pq = commerce_data["parquet"]["order_items"]
    events = _load_order_items(duckdb_con, pq)
    got = {(r["company_id"], r["product_code"]): (round(r["revenue"], 6), r["n"])
           for r in submission.revenue_by_product(events)}
    oracle_rows = duckdb_con.execute(
        """
        SELECT company_id, product_code, sum(line_amount) AS revenue, count(*) AS n
        FROM (SELECT DISTINCT company_id, order_item_id, order_id, product_code,
                              qty, line_amount
              FROM read_parquet(?))
        GROUP BY company_id, product_code
        """,
        [str(pq)],
    ).fetchall()
    expected = {(c, p): (round(float(rev), 6), int(n))
                for (c, p, rev, n) in oracle_rows}
    assert got == expected


def test_second_pass_changes_nothing(submission, duckdb_con, commerce_data):
    pq = commerce_data["parquet"]["order_items"]
    events = _load_order_items(duckdb_con, pq)
    once = submission.dedup(events, ["company_id", "order_item_id"])
    twice = submission.dedup(once, ["company_id", "order_item_id"])
    assert once == twice  # f(f(x)) == f(x)


# ---------------------------------------------------------------------------
# apply_idempotent
# ---------------------------------------------------------------------------
def test_apply_is_first_write_wins_and_noop_on_replay(submission):
    e1 = {"id": 1, "v": "a"}
    e2 = {"id": 1, "v": "b"}   # same key, different payload -> must be ignored
    s0 = {}
    s1 = submission.apply_idempotent(s0, [e1], ["id"])
    s2 = submission.apply_idempotent(s1, [e1], ["id"])  # replay: no-op
    s3 = submission.apply_idempotent(s2, [e2], ["id"])  # conflicting: ignored
    assert s1[(1,)]["v"] == "a"
    assert s2 == s1
    assert s3[(1,)]["v"] == "a"


def test_apply_does_not_mutate_input(submission):
    s0 = {(9,): {"id": 9, "v": "x"}}
    before = copy.deepcopy(s0)
    submission.apply_idempotent(s0, [{"id": 1, "v": "y"}], ["id"])
    assert s0 == before, "apply_idempotent must not mutate the input state"


# ---------------------------------------------------------------------------
# Hypothesis: idempotency and order-independence
# ---------------------------------------------------------------------------
rows_strategy = st.lists(
    st.fixed_dictionaries({
        "k": st.integers(min_value=0, max_value=20),
        "payload": st.integers(min_value=0, max_value=5),
    }),
    max_size=100,
)


@settings(max_examples=200, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(rows=rows_strategy)
def test_dedup_is_idempotent(submission, rows):
    once = submission.dedup([dict(r) for r in rows], ["k"])
    twice = submission.dedup([dict(r) for r in once], ["k"])
    assert once == twice


@settings(max_examples=200, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(rows=rows_strategy)
def test_dedup_keys_are_order_independent(submission, rows):
    import random
    forward = submission.dedup([dict(r) for r in rows], ["k"])
    shuffled = [dict(r) for r in rows]
    random.Random(123).shuffle(shuffled)
    backward = submission.dedup(shuffled, ["k"])
    # The SET of kept keys is independent of arrival order.
    assert {r["k"] for r in forward} == {r["k"] for r in backward}
    # And exactly one row per distinct key is kept.
    assert len({r["k"] for r in forward}) == len(forward)


@settings(max_examples=150, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(rows=rows_strategy)
def test_apply_idempotent_twice_equals_once(submission, rows):
    events = [{"id": r["k"], "payload": r["payload"]} for r in rows]
    s1 = submission.apply_idempotent({}, events, ["id"])
    s2 = submission.apply_idempotent(s1, events, ["id"])
    assert s1 == s2
