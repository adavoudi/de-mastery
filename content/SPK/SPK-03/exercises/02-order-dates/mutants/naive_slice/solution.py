"""Mutant: hand-slices order_ts_raw into date parts instead of parsing to a timestamp.

On the seed data (all +00:00) this returns the correct answer and used to pass every
test. It is wrong per the README ('do not hand-slice ... the timezone-naive bug') and
is now killed by test_non_utc_offset_is_normalized, which feeds a -05:00 offset row.
Added by the Phase-1 audit as a regression guard for the strengthened test.
"""

from pyspark.sql import functions as F


def with_date_parts(df):
    s = F.col("order_ts_raw")
    return df.select(
        "order_id",
        F.substring(s, 1, 10).alias("order_date"),            # WRONG: ignores offset
        F.substring(s, 1, 4).cast("int").alias("order_year"),
        F.substring(s, 6, 2).cast("int").alias("order_month"),
        F.substring(s, 9, 2).cast("int").alias("order_day"),
        F.substring(s, 12, 2).cast("int").alias("order_hour"),  # local hour, not UTC
    )
