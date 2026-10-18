"""SPK-03 / 02-order-dates — alt: go through a Unix epoch, then from_unixtime."""

from pyspark.sql import functions as F


def with_date_parts(df):
    # Parse to timestamp, convert to epoch seconds, and back — exercises the epoch path
    # while landing on the same instants.
    epoch = F.unix_timestamp(F.to_timestamp("order_ts_raw"))
    ts = F.to_timestamp(F.from_unixtime(epoch))
    return (
        df.withColumn("_ts", ts)
          .select(
              "order_id",
              F.date_format("_ts", "yyyy-MM-dd").alias("order_date"),
              F.year("_ts").alias("order_year"),
              F.month("_ts").alias("order_month"),
              F.dayofmonth("_ts").alias("order_day"),
              F.hour("_ts").alias("order_hour"),
          )
    )
