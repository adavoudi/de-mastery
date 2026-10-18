"""Mutant: hand-slices the date instead of parsing, truncating to year-month."""

from pyspark.sql import functions as F


def with_date_parts(df):
    ts = F.to_timestamp("order_ts_raw")
    return df.select(
        "order_id",
        # WRONG: slices the first 7 chars -> 'yyyy-MM', not 'yyyy-MM-dd'.
        F.substring("order_ts_raw", 1, 7).alias("order_date"),
        F.year(ts).alias("order_year"),
        F.month(ts).alias("order_month"),
        F.dayofmonth(ts).alias("order_day"),
        F.hour(ts).alias("order_hour"),
    )
