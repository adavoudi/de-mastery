"""Mutant: n_amounts uses count(*), so NULL-amount rows are wrongly counted."""

from pyspark.sql import functions as F


def payment_summary(df):
    return df.groupBy("company_id").agg(
        F.count(F.lit(1)).alias("n_payments"),
        F.count(F.lit(1)).alias("n_amounts"),  # WRONG: should ignore NULL amounts
        F.coalesce(F.sum("amount"), F.lit(0.0)).alias("total_amount"),
    )
