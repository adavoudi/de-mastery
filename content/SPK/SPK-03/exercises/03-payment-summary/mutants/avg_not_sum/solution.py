"""Mutant: total_amount uses avg instead of sum."""

from pyspark.sql import functions as F


def payment_summary(df):
    return df.groupBy("company_id").agg(
        F.count(F.lit(1)).alias("n_payments"),
        F.count("amount").alias("n_amounts"),
        F.coalesce(F.avg("amount"), F.lit(0.0)).alias("total_amount"),  # WRONG: avg
    )
