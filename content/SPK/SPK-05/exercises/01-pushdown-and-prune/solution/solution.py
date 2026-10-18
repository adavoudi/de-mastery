"""SPK-05 / 01-pushdown-and-prune — reference solution."""

from pyspark.sql import functions as F


def usd_orders(df):
    # Native predicate + explicit projection: Catalyst pushes the filter into the
    # scan (PushedFilters) and prunes to the two referenced columns (ReadSchema).
    return df.filter(F.col("currency") == "USD").select("order_id", "currency")
