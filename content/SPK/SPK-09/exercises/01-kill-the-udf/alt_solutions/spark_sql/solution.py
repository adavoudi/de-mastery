"""SPK-09 / 01-kill-the-udf — alternate: Spark SQL.

Same result expressed as a SQL join against temporary views. No Python UDF.
"""

from pyspark.sql import functions as F


def to_usd(payments, fx_rates):
    spark = payments.sparkSession
    (payments
     .withColumn("rate_date", F.to_date(F.to_timestamp("paid_ts_utc")).cast("string"))
     .createOrReplaceTempView("p_spk09_01"))
    fx_rates.createOrReplaceTempView("fx_spk09_01")
    return spark.sql(
        """
        SELECT p.payment_id,
               p.currency,
               p.amount,
               p.amount * f.usd_per_unit AS usd_amount
        FROM p_spk09_01 p
        LEFT JOIN fx_spk09_01 f
          ON p.currency = f.currency AND p.rate_date = f.rate_date
        """
    )
