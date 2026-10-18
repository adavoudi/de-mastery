"""SPK-07 / 02 — alt: coalesce first, then filter. Both are narrow, so no
Exchange is added and the partition count is num_files either way."""

from pyspark.sql import functions as F


def reduce_usd(df, num_files):
    return df.coalesce(num_files).filter(F.col("currency") == "USD")
