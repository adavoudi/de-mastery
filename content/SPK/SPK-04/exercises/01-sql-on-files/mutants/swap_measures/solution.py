"""Mutant: swaps the revenue and units expressions."""

_KEY = ["company_id", "order_item_id", "order_id", "product_code", "qty", "line_amount"]


def company_revenue(spark, order_items_path):
    spark.read.parquet(order_items_path).dropDuplicates(_KEY).createOrReplaceTempView("items")
    return spark.sql(
        """
        SELECT company_id, SUM(qty) AS revenue, SUM(line_amount) AS units
        FROM items GROUP BY company_id
        """
    )
