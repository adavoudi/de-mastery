"""SPK-03 / 02-order-dates — reference solution."""

from pyspark.sql import functions as F


def with_date_parts(df):
    ts = F.to_timestamp("order_ts_raw")
    return df.select(
        "order_id",
        F.date_format(F.to_date(ts), "yyyy-MM-dd").alias("order_date"),
        F.year(ts).alias("order_year"),
        F.month(ts).alias("order_month"),
        F.dayofmonth(ts).alias("order_day"),
        F.hour(ts).alias("order_hour"),
    )
