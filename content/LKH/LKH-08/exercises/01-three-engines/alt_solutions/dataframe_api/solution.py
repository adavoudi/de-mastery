"""LKH-08 / 01-three-engines — alt solution 1 (DataFrame API + DuckDB relation).

Genuinely different: builds the Spark query with the DataFrame API instead of SQL
text, and the DuckDB side with the relational API. Same logical query, so the
Iceberg scan still pushes both predicates.
"""

from pyspark.sql import functions as F


def run_spark(spark, table):
    return (
        spark.table(table)
        .where((F.col("region") == "r1") & (F.col("amt") > 50))
        .groupBy("cat")
        .agg(F.sum("amt").alias("total"))
    )


def run_duckdb(con, parquet_glob):
    rel = con.read_parquet(parquet_glob, hive_partitioning=True)
    return rel.filter("region = 'r1' AND amt > 50").aggregate(
        "cat, SUM(amt) AS total", "cat"
    ).arrow()
