"""Mutant: fills missing usd_amount with 0.0 instead of leaving it NULL."""

from pyspark.sql import functions as F


def to_usd(payments, fx_rates):
    p = payments.withColumn(
        "rate_date", F.to_date(F.to_timestamp("paid_ts_utc")).cast("string")
    )
    joined = p.join(F.broadcast(fx_rates), on=["currency", "rate_date"], how="left")
    return joined.select(
        "payment_id", "currency", "amount",
        # WRONG: coalesce to 0 hides missing amounts/rates as real zero conversions.
        F.coalesce(F.col("amount") * F.col("usd_per_unit"), F.lit(0.0)).alias("usd_amount"),
    )
