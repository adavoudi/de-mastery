"""Tests for SPK-06 / 03-broadcast-var-accumulator.

Data: valid_df equals the allowlisted-currency rows (DuckDB oracle, G6).
Counter: invalid_count equals the number of non-allowlisted rows, and
valid + invalid == total.
"""

from de_testkit import assert_frames_equal

ALLOW = ["USD", "EUR", "GBP", "JPY"]
_ALLOW_SQL = ", ".join(f"'{c}'" for c in ALLOW)


def _orders(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["orders"]))


def _valid_oracle(con, path):
    return con.execute(
        f"SELECT * FROM read_parquet('{path}') WHERE currency IN ({_ALLOW_SQL})"
    ).arrow()


def _invalid_count(con, path):
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{path}') WHERE currency NOT IN ({_ALLOW_SQL})"
    ).fetchone()[0]


def test_valid_df_matches_oracle(submission, spark, duckdb_con, commerce_data):
    orders = _orders(spark, commerce_data)
    valid_df, _ = submission.validate_currencies(spark, orders, ALLOW)
    assert_frames_equal(valid_df, _valid_oracle(duckdb_con, commerce_data["parquet"]["orders"]))


def test_invalid_count_matches_oracle(submission, spark, duckdb_con, commerce_data):
    orders = _orders(spark, commerce_data)
    _, invalid_count = submission.validate_currencies(spark, orders, ALLOW)
    assert invalid_count == _invalid_count(duckdb_con, commerce_data["parquet"]["orders"])


def test_valid_plus_invalid_equals_total(submission, spark, commerce_data):
    orders = _orders(spark, commerce_data)
    valid_df, invalid_count = submission.validate_currencies(spark, orders, ALLOW)
    assert valid_df.count() + invalid_count == orders.count()


def test_valid_schema_preserved(submission, spark, commerce_data):
    orders = _orders(spark, commerce_data)
    valid_df, _ = submission.validate_currencies(spark, orders, ALLOW)
    assert set(valid_df.columns) == set(orders.columns)
