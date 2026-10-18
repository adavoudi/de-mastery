"""Tests for SPK-09 / 01-kill-the-udf.

G6 oracle: DuckDB performs the same date-matched left join + multiply over the same
seeded tables. Plan gate: the physical plan must not contain BatchEvalPython (a plain
Python UDF). Native functions, pandas UDFs and Arrow UDFs all pass; a plain udf fails.
"""

from de_testkit import assert_frames_equal, plan_of


def _oracle(con, payments_path, fx_path):
    return con.execute(
        f"""
        SELECT p.payment_id,
               p.currency,
               p.amount,
               p.amount * f.usd_per_unit AS usd_amount
        FROM read_parquet('{payments_path}') p
        LEFT JOIN read_parquet('{fx_path}') f
          ON p.currency = f.currency
         AND CAST(CAST(p.paid_ts_utc AS TIMESTAMP) AS DATE) = CAST(f.rate_date AS DATE)
        """
    ).arrow()


def _inputs(spark, commerce_data):
    payments = spark.read.parquet(str(commerce_data["parquet"]["payments"]))
    fx = spark.read.parquet(str(commerce_data["parquet"]["fx_rates"]))
    return payments, fx


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    payments, fx = _inputs(spark, commerce_data)
    out = submission.to_usd(payments, fx)
    oracle = _oracle(duckdb_con, str(commerce_data["parquet"]["payments"]),
                     str(commerce_data["parquet"]["fx_rates"]))
    assert_frames_equal(out, oracle, rel_tol=1e-6)


def test_expected_columns(submission, spark, commerce_data):
    payments, fx = _inputs(spark, commerce_data)
    out = submission.to_usd(payments, fx)
    assert set(out.columns) == {"payment_id", "currency", "amount", "usd_amount"}


def test_every_payment_survives(submission, spark, commerce_data):
    # A left join must keep every payment; an inner join (mutant) drops bad-currency rows.
    payments, fx = _inputs(spark, commerce_data)
    out = submission.to_usd(payments, fx)
    assert out.count() == payments.count()
    # payment_id is unique in the input, so it stays unique in the output.
    assert out.select("payment_id").distinct().count() == payments.count()


def test_no_batch_eval_python(submission, spark, commerce_data):
    # The whole point: no plain Python UDF (BatchEvalPython) in the plan.
    payments, fx = _inputs(spark, commerce_data)
    out = submission.to_usd(payments, fx)
    plan = plan_of(out)
    assert "BatchEvalPython" not in plan, plan


def test_null_amount_stays_null(submission, spark, commerce_data):
    # A NULL amount must yield a NULL usd_amount, never a fabricated 0.
    payments, fx = _inputs(spark, commerce_data)
    out = submission.to_usd(payments, fx)
    null_amt_ids = {r["payment_id"] for r in
                    payments.filter(payments["amount"].isNull()).select("payment_id").collect()}
    if null_amt_ids:
        bad = out.filter(out["payment_id"].isin(list(null_amt_ids)))
        assert bad.filter(bad["usd_amount"].isNotNull()).count() == 0
