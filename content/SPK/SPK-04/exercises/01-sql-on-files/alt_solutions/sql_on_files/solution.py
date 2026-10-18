"""SPK-04 / 01-sql-on-files — alt: query the file directly, dedup with DISTINCT in SQL."""


def company_revenue(spark, order_items_path):
    # SQL on files: read the path directly, then a temp view over the DISTINCT rows.
    raw = spark.sql(f"SELECT * FROM parquet.`{order_items_path}`")
    raw.distinct().createOrReplaceTempView("items_distinct")
    return spark.sql(
        """
        SELECT company_id,
               SUM(line_amount) AS revenue,
               SUM(qty)         AS units
        FROM items_distinct
        GROUP BY company_id
        """
    )
