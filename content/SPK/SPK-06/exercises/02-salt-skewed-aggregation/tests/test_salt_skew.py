"""Tests for SPK-06 / 02-salt-skewed-aggregation.

Data: DuckDB plain (unsalted) sum per company is the oracle (G6).
Invariant: salting must be result-neutral, so num_salts=1 and num_salts=8 give
identical per-company totals, and both equal the oracle.
Structure: exactly one row per company.
"""

from de_testkit import assert_frames_equal


def _oracle(con, path):
    return con.execute(
        f"""
        SELECT company_id, sum(line_amount) AS total
        FROM read_parquet('{path}')
        GROUP BY company_id
        """
    ).arrow()


def _items(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["order_items"]))


def test_matches_oracle_8_salts(submission, spark, duckdb_con, commerce_data):
    items = _items(spark, commerce_data)
    out = submission.salted_totals(items, 8)
    assert_frames_equal(out, _oracle(duckdb_con, commerce_data["parquet"]["order_items"]),
                        rel_tol=1e-6)


def test_matches_oracle_1_salt(submission, spark, duckdb_con, commerce_data):
    items = _items(spark, commerce_data)
    out = submission.salted_totals(items, 1)
    assert_frames_equal(out, _oracle(duckdb_con, commerce_data["parquet"]["order_items"]),
                        rel_tol=1e-6)


def test_salt_count_is_result_neutral(submission, spark, commerce_data):
    items = _items(spark, commerce_data)
    a = {r["company_id"]: r["total"] for r in submission.salted_totals(items, 1).collect()}
    b = {r["company_id"]: r["total"] for r in submission.salted_totals(items, 8).collect()}
    assert a.keys() == b.keys()
    for k in a:
        assert abs(a[k] - b[k]) < 1e-6, (k, a[k], b[k])


def test_one_row_per_company(submission, spark, commerce_data):
    items = _items(spark, commerce_data)
    out = submission.salted_totals(items, 8)
    assert set(out.columns) == {"company_id", "total"}
    ids = [r["company_id"] for r in out.collect()]
    assert len(ids) == len(set(ids)), f"duplicate company rows: {ids}"
