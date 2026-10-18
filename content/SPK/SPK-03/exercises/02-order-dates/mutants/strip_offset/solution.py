"""Mutant: strips the offset and parses the local wall-clock time as if it were UTC."""

from pyspark.sql import functions as F


def with_date_parts(df):
    # WRONG: drops '+02:00' / '-05:00', so local time is mislabeled as UTC.
    ts = F.to_timestamp(F.substring("order_ts_raw", 1, 19))
    return df.select(
        "order_id",
        F.date_format(F.to_date(ts), "yyyy-MM-dd").alias("order_date"),
        F.year(ts).alias("order_year"),
        F.month(ts).alias("order_month"),
        F.dayofmonth(ts).alias("order_day"),
        F.hour(ts).alias("order_hour"),
    )
