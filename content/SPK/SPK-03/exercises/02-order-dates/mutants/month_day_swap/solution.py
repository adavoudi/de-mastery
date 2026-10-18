"""Mutant: swaps month and day extraction."""

from pyspark.sql import functions as F


def with_date_parts(df):
    ts = F.to_timestamp("order_ts_raw")
    return df.select(
        "order_id",
        F.date_format(F.to_date(ts), "yyyy-MM-dd").alias("order_date"),
        F.year(ts).alias("order_year"),
        F.dayofmonth(ts).alias("order_month"),  # WRONG: this is the day
        F.month(ts).alias("order_day"),          # WRONG: this is the month
        F.hour(ts).alias("order_hour"),
    )
