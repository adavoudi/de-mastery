"""Mutant: uses repartition instead of coalesce — adds an Exchange (full shuffle)
to the plan, which the no-added-shuffle assertion rejects."""

from pyspark.sql import functions as F


def reduce_usd(df, num_files):
    return df.filter(F.col("currency") == "USD").repartition(num_files)
