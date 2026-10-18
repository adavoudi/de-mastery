"""SPK-04 / 01-sql-on-files — alt: DataFrame API, temp view used only as the ref point."""

from pyspark.sql import functions as F

_KEY = ["company_id", "order_item_id", "order_id", "product_code", "qty", "line_amount"]


def company_revenue(spark, order_items_path):
    df = spark.read.parquet(order_items_path).dropDuplicates(_KEY)
    df.createOrReplaceTempView("items")  # register the ref, then aggregate via the API
    items = spark.table("items")
    return items.groupBy("company_id").agg(
        F.sum("line_amount").alias("revenue"),
        F.sum("qty").alias("units"),
    )
