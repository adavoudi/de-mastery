"""Mutant: units counts rows instead of summing qty."""

_KEY = ["company_id", "order_item_id", "order_id", "product_code", "qty", "line_amount"]


def company_revenue(spark, order_items_path):
    spark.read.parquet(order_items_path).dropDuplicates(_KEY).createOrReplaceTempView("items")
    return spark.sql(
        """
        SELECT company_id, SUM(line_amount) AS revenue, COUNT(*) AS units
        FROM items GROUP BY company_id
        """
    )
