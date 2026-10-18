"""SPK-07 / 02-coalesce-no-shuffle — reference solution."""

from pyspark.sql import functions as F


def reduce_usd(df, num_files):
    return df.filter(F.col("currency") == "USD").coalesce(num_files)
