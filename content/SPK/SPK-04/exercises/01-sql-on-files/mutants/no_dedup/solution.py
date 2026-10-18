"""Mutant: skips dedup, so duplicate line rows inflate revenue and units."""


def company_revenue(spark, order_items_path):
    spark.read.parquet(order_items_path).createOrReplaceTempView("items")  # no dedup
    return spark.sql(
        """
        SELECT company_id, SUM(line_amount) AS revenue, SUM(qty) AS units
        FROM items GROUP BY company_id
        """
    )
