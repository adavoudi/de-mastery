"""Mutant: n_payments uses count('amount'), undercounting NULL-amount rows."""

from pyspark.sql import functions as F


def payment_summary(df):
    return df.groupBy("company_id").agg(
        F.count("amount").alias("n_payments"),  # WRONG: skips NULL-amount payments
        F.count("amount").alias("n_amounts"),
        F.coalesce(F.sum("amount"), F.lit(0.0)).alias("total_amount"),
    )
