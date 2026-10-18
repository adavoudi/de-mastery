"""Tests for SPK-03 / 02-order-dates.

Oracle: DuckDB parses the same order_ts_raw string (offset-aware) and and extracts the parts independently.
assert_frames_equal ignores row/column order.
"""

from de_testkit import assert_frames_equal


def _oracle(con, path):
    return con.execute(
        f"""
        SELECT
          order_id,
          strftime(order_ts_raw::timestamptz AT TIME ZONE 'UTC', '%Y-%m-%d') AS order_date,
          extract(year  FROM order_ts_raw::timestamptz AT TIME ZONE 'UTC')::int AS order_year,
          extract(month FROM order_ts_raw::timestamptz AT TIME ZONE 'UTC')::int AS order_month,
          extract(day   FROM order_ts_raw::timestamptz AT TIME ZONE 'UTC')::int AS order_day,
          extract(hour  FROM order_ts_raw::timestamptz AT TIME ZONE 'UTC')::int AS order_hour
        FROM read_parquet('{path}')
        """
    ).arrow()


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    path = str(commerce_data["parquet"]["orders"])
    raw = spark.read.parquet(path).drop("order_ts_utc")
    out = submission.with_date_parts(raw)
    assert_frames_equal(out, _oracle(duckdb_con, path))


def test_expected_columns(submission, spark, commerce_data):
    raw = spark.read.parquet(str(commerce_data["parquet"]["orders"])).drop("order_ts_utc")
    out = submission.with_date_parts(raw)
    assert set(out.columns) == {
        "order_id", "order_date", "order_year", "order_month",
        "order_day", "order_hour",
    }


def test_no_null_parts(submission, spark, commerce_data):
    # Every order has a valid order_ts_raw, so no extracted part may be NULL.
    raw = spark.read.parquet(str(commerce_data["parquet"]["orders"])).drop("order_ts_utc")
    out = submission.with_date_parts(raw)
    for c in ("order_date", "order_year", "order_month", "order_day", "order_hour"):
        assert out.filter(out[c].isNull()).count() == 0, f"{c} has NULLs"


def test_date_string_format(submission, spark, commerce_data):
    import re
    raw = spark.read.parquet(str(commerce_data["parquet"]["orders"])).drop("order_ts_utc")
    out = submission.with_date_parts(raw)
    for row in out.select("order_date").collect():
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["order_date"]), row["order_date"]


def test_non_utc_offset_is_normalized(submission, spark):
    """Enforce the README's 'parse, don't hand-slice' rule.

    The seed data stores only +00:00 offsets, so a solution that *slices* characters
    out of order_ts_utc gets the right answer on the seed and slips past the oracle.
    Here we feed a synthetic row whose offset is NOT UTC (-05:00). A correct parse
    converts it to UTC (session tz is UTC) and reports hour 20 / day 8; naive string
    slicing wrongly reports hour 15. This kills the 'naive_slice' mutant.
    """
    # 2026-01-08T15:03:09-05:00  ==  2026-01-08T20:03:09Z
    df = spark.createDataFrame(
        [(999999, "2026-01-08T15:03:09-05:00")],
        ["order_id", "order_ts_raw"],
    )
    out = submission.with_date_parts(df)
    row = out.filter(out["order_id"] == 999999).collect()[0]
    assert row["order_hour"] == 20, (
        f"expected UTC-normalized hour 20, got {row['order_hour']} "
        "(string slicing / timezone-naive parse?)"
    )
    assert row["order_day"] == 8, row["order_day"]
    assert row["order_month"] == 1, row["order_month"]
    assert row["order_year"] == 2026, row["order_year"]
    assert row["order_date"] == "2026-01-08", row["order_date"]
