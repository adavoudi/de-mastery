"""Mutant: inner join drops payments whose currency/date has no rate (e.g. bad currency codes)."""

from pyspark.sql import functions as F


def to_usd(payments, fx_rates):
    p = payments.withColumn(
        "rate_date", F.to_date(F.to_timestamp("paid_ts_utc")).cast("string")
    )
    joined = p.join(F.broadcast(fx_rates), on=["currency", "rate_date"], how="inner")  # WRONG
    return joined.select(
        "payment_id", "currency", "amount",
        (F.col("amount") * F.col("usd_per_unit")).alias("usd_amount"),
    )
