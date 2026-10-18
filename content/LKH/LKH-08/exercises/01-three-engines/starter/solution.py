"""LKH-08 / 01-three-engines — starter. See README.md.

Run the SAME query on Spark (Iceberg) and DuckDB (Parquet) and return results with
columns cat, total. Then fill plan_facts.yaml from the real Spark plan.
"""


def run_spark(spark, table):
    # TODO: SELECT cat, SUM(amt) AS total FROM <table>
    #       WHERE region='r1' AND amt > 50 GROUP BY cat
    raise NotImplementedError


def run_duckdb(con, parquet_glob):
    # TODO: the same query over read_parquet(parquet_glob, hive_partitioning=true)
    raise NotImplementedError
