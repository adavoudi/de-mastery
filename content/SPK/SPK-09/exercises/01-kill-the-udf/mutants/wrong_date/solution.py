"""Mutant: converts everything at the first day's rate instead of the payment's own date."""

from pyspark.sql import functions as F


def to_usd(payments, fx_rates):
    # WRONG: a single fixed date for all payments, not each payment's own date.
    p = payments.withColumn("rate_date", F.lit("2026-01-01"))
    joined = p.join(F.broadcast(fx_rates), on=["currency", "rate_date"], how="left")
    return joined.select(
        "payment_id", "currency", "amount",
        (F.col("amount") * F.col("usd_per_unit")).alias("usd_amount"),
    )
