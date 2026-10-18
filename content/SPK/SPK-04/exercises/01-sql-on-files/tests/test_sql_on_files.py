"""Tests for SPK-04 / 01-sql-on-files.

Oracle: DuckDB dedups the same order_items file and aggregates independently.
"""

from de_testkit import assert_frames_equal


def _oracle(con, path):
    return con.execute(
        f"""
        WITH ded AS (SELECT DISTINCT * FROM read_parquet('{path}'))
        SELECT company_id,
               SUM(line_amount) AS revenue,
               SUM(qty)         AS units
        FROM ded
        GROUP BY company_id
        """
    ).arrow()


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    path = str(commerce_data["parquet"]["order_items"])
    out = submission.company_revenue(spark, path)
    assert_frames_equal(out, _oracle(duckdb_con, path), rel_tol=1e-6)


def test_expected_columns(submission, spark, commerce_data):
    path = str(commerce_data["parquet"]["order_items"])
    out = submission.company_revenue(spark, path)
    assert set(out.columns) == {"company_id", "revenue", "units"}


def test_dedup_applied(submission, spark, duckdb_con, commerce_data):
    # Revenue must equal the deduped sum, strictly less than the raw (with-duplicate) sum
    # for at least one company, proving dedup happened.
    path = str(commerce_data["parquet"]["order_items"])
    out = submission.company_revenue(spark, path)
    got = {r["company_id"]: r["revenue"] for r in out.collect()}
    raw = dict(duckdb_con.execute(
        f"SELECT company_id, SUM(line_amount) FROM read_parquet('{path}') GROUP BY 1"
    ).fetchall())
    ded = dict(duckdb_con.execute(
        f"WITH d AS (SELECT DISTINCT * FROM read_parquet('{path}')) "
        f"SELECT company_id, SUM(line_amount) FROM d GROUP BY 1"
    ).fetchall())
    # there is at least one duplicate in the dataset, so some company's raw > deduped
    assert any(raw[c] > ded[c] + 1e-9 for c in raw), "fixture has no duplicate to test"
    for c, v in got.items():
        assert abs(v - ded[c]) < 1e-6, f"{c}: revenue {v} != deduped {ded[c]}"


def test_one_row_per_company(submission, spark, commerce_data):
    path = str(commerce_data["parquet"]["order_items"])
    out = submission.company_revenue(spark, path)
    assert out.count() == out.select("company_id").distinct().count()
