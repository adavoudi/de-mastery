"""SPK-03 / 02-order-dates — alt: Spark SQL with built-in date functions."""


def with_date_parts(df):
    df.createOrReplaceTempView("o")
    return df.sparkSession.sql(
        """
        SELECT
          order_id,
          date_format(to_date(to_timestamp(order_ts_raw)), 'yyyy-MM-dd') AS order_date,
          year(to_timestamp(order_ts_raw))       AS order_year,
          month(to_timestamp(order_ts_raw))      AS order_month,
          dayofmonth(to_timestamp(order_ts_raw)) AS order_day,
          hour(to_timestamp(order_ts_raw))       AS order_hour
        FROM o
        """
    )
