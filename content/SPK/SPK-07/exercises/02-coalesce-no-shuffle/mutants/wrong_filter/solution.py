"""Mutant: filters != 'EUR' instead of == 'USD', keeping GBP/JPY/bad-code rows —
data oracle fails."""

from pyspark.sql import functions as F


def reduce_usd(df, num_files):
    return df.filter(F.col("currency") != "EUR").coalesce(num_files)
