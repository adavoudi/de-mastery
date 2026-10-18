"""Tests for FND-01/01-scale-up-vs-out.

Oracle (G6): DuckDB computes the expected per-product aggregation independently over
both the Parquet and the CSV input. The two learner implementations must match the
oracle AND each other. We assert a robust MEASURED quantity (bytes read), never
wall-clock time.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def _submission_dir() -> Path:
    d = os.environ.get("DE_SUBMISSION_DIR")
    assert d, "DE_SUBMISSION_DIR must be set by de test / de validate"
    return Path(d)


def _as_map(rows):
    """product_code -> (round(total,6), n)."""
    out = {}
    for r in rows:
        out[r["product_code"]] = (round(float(r["total_amount"]), 6), int(r["n"]))
    return out


def _oracle_parquet(con, parquet_path):
    rows = con.execute(
        """
        SELECT product_code, sum(line_amount) AS total_amount, count(*) AS n
        FROM read_parquet(?)
        GROUP BY product_code
        """,
        [str(parquet_path)],
    ).fetchall()
    return {pc: (round(float(ta), 6), int(n)) for (pc, ta, n) in rows}


def _oracle_csv(con, csv_path):
    rows = con.execute(
        """
        SELECT product_code, sum(line_amount) AS total_amount, count(*) AS n
        FROM read_csv_auto(?, header=true)
        GROUP BY product_code
        """,
        [str(csv_path)],
    ).fetchall()
    return {pc: (round(float(ta), 6), int(n)) for (pc, ta, n) in rows}


def test_duckdb_matches_oracle(submission, duckdb_con, commerce_data):
    pq_path = commerce_data["parquet"]["order_items"]
    out = submission.agg_duckdb(pq_path)
    assert _as_map(out) == _oracle_parquet(duckdb_con, pq_path)


def test_pool_matches_oracle(submission, duckdb_con, commerce_data):
    csv_path = commerce_data["csv"]["order_items"]
    out = submission.agg_pool(csv_path, 4)
    assert _as_map(out) == _oracle_csv(duckdb_con, csv_path)


def test_two_engines_agree(submission, commerce_data):
    pq_path = commerce_data["parquet"]["order_items"]
    csv_path = commerce_data["csv"]["order_items"]
    a = _as_map(submission.agg_duckdb(pq_path))
    b = _as_map(submission.agg_pool(csv_path, 3))
    assert a == b, "DuckDB and the process pool disagree on the aggregation"


def test_pool_result_independent_of_worker_count(submission, commerce_data):
    """Combine must be associative: answer cannot depend on the number of workers."""
    csv_path = commerce_data["csv"]["order_items"]
    r1 = _as_map(submission.agg_pool(csv_path, 1))
    r5 = _as_map(submission.agg_pool(csv_path, 5))
    assert r1 == r5


def test_columnar_reads_fewer_bytes(submission, commerce_data):
    pq_path = commerce_data["parquet"]["order_items"]
    csv_path = commerce_data["csv"]["order_items"]
    br = submission.bytes_read(pq_path, csv_path)
    assert set(br) == {"parquet", "csv"}
    assert br["parquet"] > 0 and br["csv"] > 0
    # The measured point of the lesson: columnar + projection reads strictly fewer bytes.
    assert br["parquet"] < br["csv"], (
        f"expected parquet bytes < csv bytes, got {br}"
    )


REQUIRED_KEYWORDS = ("amdahl", "shuffle", "bytes", "columnar", "serial")


def test_explanation_contains_required_reasoning(submission):
    # submission is requested so the fixture validates the module imports cleanly too.
    path = _submission_dir() / "explain.txt"
    assert path.exists(), "explain.txt is missing from the submission"
    text = path.read_text(encoding="utf-8").lower()
    missing = [k for k in REQUIRED_KEYWORDS if k not in text]
    assert not missing, f"explain.txt must discuss: {missing}"
