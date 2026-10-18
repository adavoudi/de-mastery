"""Tests for SPK-03 / 03-payment-summary.

Oracle: DuckDB computes the per-company summary from the same parquet, with the same
NULL semantics. Floats compared with tolerance; row/column order ignored.
"""

from de_testkit import assert_frames_equal


def _oracle(con, path):
    return con.execute(
        f"""
        SELECT
          company_id,
          count(*)                   AS n_payments,
          count(amount)              AS n_amounts,
          coalesce(sum(amount), 0.0) AS total_amount
        FROM read_parquet('{path}')
        GROUP BY company_id
        """
    ).arrow()


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    path = str(commerce_data["parquet"]["payments"])
    raw = spark.read.parquet(path)
    out = submission.payment_summary(raw)
    assert_frames_equal(out, _oracle(duckdb_con, path), rel_tol=1e-6)


def test_expected_columns(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["payments"]))
    out = submission.payment_summary(raw)
    assert set(out.columns) == {"company_id", "n_payments", "n_amounts", "total_amount"}


def test_payments_ge_amounts(submission, spark, commerce_data):
    # Every non-NULL amount is a payment, so n_payments >= n_amounts in every group.
    raw = spark.read.parquet(str(commerce_data["parquet"]["payments"]))
    out = submission.payment_summary(raw)
    for r in out.collect():
        assert r["n_payments"] >= r["n_amounts"], r


def test_total_is_never_null(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["payments"]))
    out = submission.payment_summary(raw)
    assert out.filter(out["total_amount"].isNull()).count() == 0


def test_sortable_to_python_list(submission, spark, commerce_data):
    # Objective 3.7: sort and pull a column into a Python list on the driver.
    raw = spark.read.parquet(str(commerce_data["parquet"]["payments"]))
    out = submission.payment_summary(raw).orderBy("company_id")
    ids = [r["company_id"] for r in out.select("company_id").collect()]
    assert ids == sorted(ids)
