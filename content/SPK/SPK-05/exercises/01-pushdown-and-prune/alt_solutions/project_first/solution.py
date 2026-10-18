"""SPK-05 / 01 — alt: project first, then filter. Catalyst still pushes the
predicate below the projection into the scan, so pushdown + pruning both hold."""

from pyspark.sql import functions as F


def usd_orders(df):
    return df.select("order_id", "currency").filter(F.col("currency") == "USD")
