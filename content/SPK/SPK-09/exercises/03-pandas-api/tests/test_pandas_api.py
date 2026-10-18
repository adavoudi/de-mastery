"""Tests for SPK-09 / 03-pandas-api.

G6 oracle: DuckDB computes the same per-company count + sum over the same order_items
parquet. order_items contains deliberate duplicate line rows, so n_lines must count
rows (not distinct orders). Row/column order ignored; floats use tolerance.
"""

from de_testkit import assert_frames_equal


def _oracle(con, path):
    return con.execute(
        f"""
        SELECT company_id,
               count(*)         AS n_lines,
               sum(line_amount) AS total_line_amount
        FROM read_parquet('{path}')
        GROUP BY company_id
        """
    ).arrow()


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    path = str(commerce_data["parquet"]["order_items"])
    out = submission.line_summary(spark.read.parquet(path))
    assert_frames_equal(out, _oracle(duckdb_con, path), rel_tol=1e-6)


def test_expected_columns(submission, spark, commerce_data):
    path = str(commerce_data["parquet"]["order_items"])
    out = submission.line_summary(spark.read.parquet(path))
    assert set(out.columns) == {"company_id", "n_lines", "total_line_amount"}


def test_one_row_per_company(submission, spark, commerce_data):
    path = str(commerce_data["parquet"]["order_items"])
    raw = spark.read.parquet(path)
    out = submission.line_summary(raw)
    n_companies = raw.select("company_id").distinct().count()
    assert out.count() == n_companies
    assert out.select("company_id").distinct().count() == n_companies


def test_returns_spark_dataframe(submission, spark, commerce_data):
    # Must return a Spark DataFrame (not a pandas / pandas-on-Spark object).
    path = str(commerce_data["parquet"]["order_items"])
    out = submission.line_summary(spark.read.parquet(path))
    assert "pyspark.sql" in type(out).__module__ and hasattr(out, "_jdf")
