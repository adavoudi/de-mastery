"""Tests for SPK-09 / 02-normalize-codes.

The seeded commerce feed is tiny and does not guarantee every dirty case, so the
value tests use a CONTROLLED inline set of currency codes covering: valid, lowercase,
padded, blank, and the bogus 'XXX'. The oracle (DuckDB) runs the same canonicalization
over the identical rows. The plan gate (no BatchEvalPython) also runs on the real feed.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from de_testkit import assert_frames_equal, plan_of

# (order_id, raw currency) — one row per interesting case.
_CASES = [
    (1, "USD"),      # already canonical, valid
    (2, "usd"),      # lowercase -> USD, valid
    (3, "  eur "),   # padded + lowercase -> EUR, valid
    (4, "EUR"),      # valid
    (5, "XXX"),      # bogus, invalid
    (6, ""),         # blank -> NULL, invalid
    (7, "gbp"),      # lowercase -> GBP, valid
    (8, "JPY"),      # valid
]


def _inline_df(spark):
    return spark.createDataFrame(_CASES, "order_id INT, currency STRING")


def _inline_oracle(con, path):
    return con.execute(
        f"""
        SELECT order_id,
               nullif(upper(trim(currency)), '') AS currency_clean,
               coalesce(nullif(upper(trim(currency)), '')
                        IN ('USD','EUR','GBP','JPY'), false) AS is_valid
        FROM read_parquet('{path}')
        """
    ).arrow()


def test_matches_oracle_inline(submission, spark, duckdb_con, tmp_path):
    df = _inline_df(spark)
    out = submission.clean_currency(df)
    # Write the identical rows for DuckDB to read, so both engines see the same input.
    p = str(tmp_path / "cases.parquet")
    pq.write_table(pa.table({"order_id": [c[0] for c in _CASES],
                             "currency": [c[1] for c in _CASES]}), p)
    assert_frames_equal(out, _inline_oracle(duckdb_con, p))


def test_expected_columns(submission, spark):
    out = submission.clean_currency(_inline_df(spark))
    assert set(out.columns) == {"order_id", "currency_clean", "is_valid"}


def test_is_valid_never_null(submission, spark):
    out = submission.clean_currency(_inline_df(spark))
    assert out.filter(out["is_valid"].isNull()).count() == 0


def test_blank_becomes_null(submission, spark):
    out = submission.clean_currency(_inline_df(spark))
    r = {row["order_id"]: row for row in out.collect()}
    assert r[6]["currency_clean"] is None
    assert r[6]["is_valid"] is False


def test_lowercase_canonicalized(submission, spark):
    out = submission.clean_currency(_inline_df(spark))
    r = {row["order_id"]: row for row in out.collect()}
    assert r[2]["currency_clean"] == "USD" and r[2]["is_valid"] is True
    assert r[3]["currency_clean"] == "EUR" and r[3]["is_valid"] is True
    assert r[5]["currency_clean"] == "XXX" and r[5]["is_valid"] is False


def test_no_batch_eval_python_on_feed(submission, spark, commerce_data):
    # Plan gate runs on the real feed too — no plain Python UDF allowed.
    out = submission.clean_currency(
        spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    )
    plan = plan_of(out)
    assert "BatchEvalPython" not in plan, plan
