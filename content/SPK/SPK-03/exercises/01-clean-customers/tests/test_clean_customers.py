"""Tests for SPK-03 / 01-clean-customers.

Oracle: DuckDB computes the expected clean set from the same parquet file, independently
of the learner's Spark code (G6). assert_frames_equal ignores row and column order.
"""

from de_testkit import assert_frames_equal


def _oracle(con, path):
    return con.execute(
        f"""
        SELECT DISTINCT company_id, customer_id, email
        FROM read_parquet('{path}')
        WHERE email IS NOT NULL
        """
    ).arrow()


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    path = str(commerce_data["parquet"]["customers"])
    raw = spark.read.parquet(path)
    out = submission.clean_customers(raw)
    expected = _oracle(duckdb_con, path)
    assert_frames_equal(out, expected)


def test_has_exactly_three_columns(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["customers"]))
    out = submission.clean_customers(raw)
    assert set(out.columns) == {"company_id", "customer_id", "email"}


def test_no_null_emails(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["customers"]))
    out = submission.clean_customers(raw)
    assert out.filter(out["email"].isNull()).count() == 0


def test_business_key_is_unique(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["customers"]))
    out = submission.clean_customers(raw)
    total = out.count()
    distinct_keys = out.select("company_id", "customer_id").distinct().count()
    assert total == distinct_keys, "duplicate business keys remain"


def test_does_not_overmerge_colliding_ids(submission, spark, duckdb_con, commerce_data):
    # There exist customer_ids shared across companies. The clean output must keep one
    # row per (company_id, customer_id), not collapse them to one row per customer_id.
    path = str(commerce_data["parquet"]["customers"])
    raw = spark.read.parquet(path)
    out = submission.clean_customers(raw)
    expected_rows = duckdb_con.execute(
        f"""SELECT COUNT(*) FROM (
                SELECT DISTINCT company_id, customer_id
                FROM read_parquet('{path}') WHERE email IS NOT NULL)"""
    ).fetchone()[0]
    assert out.count() == expected_rows
