"""Mutant: Spark query drops the amt > 50 predicate, so it over-counts and the
plan no longer pushes that filter."""


def run_spark(spark, table):
    # WRONG: missing the amt > 50 predicate.
    return spark.sql(
        f"SELECT cat, SUM(amt) AS total FROM {table} "
        f"WHERE region = 'r1' GROUP BY cat"
    )


def run_duckdb(con, parquet_glob):
    src = f"read_parquet('{parquet_glob}', hive_partitioning=true)"
    return con.execute(
        f"SELECT cat, SUM(amt) AS total FROM {src} "
        f"WHERE region = 'r1' AND amt > 50 GROUP BY cat"
    ).arrow()
