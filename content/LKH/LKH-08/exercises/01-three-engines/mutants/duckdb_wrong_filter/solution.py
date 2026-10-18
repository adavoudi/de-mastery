"""Mutant: DuckDB uses amt >= 50 (off-by-one boundary), so it disagrees with Spark."""


def run_spark(spark, table):
    return spark.sql(
        "SELECT cat, SUM(amt) AS total FROM {} "
        "WHERE region = 'r1' AND amt > 50 GROUP BY cat".format(table)
    )


def run_duckdb(con, parquet_glob):
    src = f"read_parquet('{parquet_glob}', hive_partitioning=true)"
    # WRONG: > 48 instead of > 50 includes the amt == 49 row (which is in region r1).
    return con.execute(
        f"SELECT cat, SUM(amt) AS total FROM {src} "
        f"WHERE region = 'r1' AND amt > 48 GROUP BY cat"
    ).arrow()
