"""SPK-08 / 02 — alt: set the AQE configs via SQL SET and aggregate in SQL."""


def coalesced_aggregate(spark, order_items):
    spark.sql("SET spark.sql.adaptive.enabled=true")
    spark.sql("SET spark.sql.adaptive.coalescePartitions.enabled=true")
    spark.sql("SET spark.sql.shuffle.partitions=64")
    spark.sql("SET spark.sql.adaptive.coalescePartitions.minPartitionNum=1")
    order_items.createOrReplaceTempView("oi")
    return spark.sql(
        "SELECT company_id, SUM(line_amount) AS total FROM oi GROUP BY company_id"
    )
