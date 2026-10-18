"""SPK-03 / 03-payment-summary — alt: conditional sums instead of count/coalesce."""

from pyspark.sql import functions as F


def payment_summary(df):
    # n_amounts via a conditional sum (1 when amount present), total via na.fill(0).
    present = F.when(F.col("amount").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    filled = df.withColumn("_amt0", F.coalesce(F.col("amount"), F.lit(0.0)))
    return filled.groupBy("company_id").agg(
        F.sum(F.lit(1)).alias("n_payments"),
        F.sum(present).alias("n_amounts"),
        F.sum("_amt0").alias("total_amount"),
    )
