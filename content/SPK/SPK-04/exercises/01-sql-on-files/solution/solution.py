"""SPK-04 / 01-sql-on-files — reference solution (temp view + spark.sql)."""

_KEY = ["company_id", "order_item_id", "order_id", "product_code", "qty", "line_amount"]


def company_revenue(spark, order_items_path):
    deduped = spark.read.parquet(order_items_path).dropDuplicates(_KEY)
    deduped.createOrReplaceTempView("items")
    return spark.sql(
        """
        SELECT company_id,
               SUM(line_amount) AS revenue,
               SUM(qty)         AS units
        FROM items
        GROUP BY company_id
        """
    )
