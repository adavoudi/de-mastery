"""Mutant: forgets to reduce the partition count, so the result keeps the base's
many partitions instead of num_files."""

from pyspark.sql import functions as F


def reduce_usd(df, num_files):
    return df.filter(F.col("currency") == "USD")
