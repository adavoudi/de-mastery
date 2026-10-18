"""LKH-08 / 01-three-engines — reference solution."""

QUERY = (
    "SELECT cat, SUM(amt) AS total FROM {src} "
    "WHERE region = 'r1' AND amt > 50 GROUP BY cat"
)


def run_spark(spark, table):
    return spark.sql(QUERY.format(src=table))


def run_duckdb(con, parquet_glob):
    src = f"read_parquet('{parquet_glob}', hive_partitioning=true)"
    return con.execute(QUERY.format(src=src)).arrow()
