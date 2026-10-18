"""LKH-08 / 01-three-engines — alt solution 2 (chained filters + a temp view).

Genuinely different structure: Spark registers a temp view and filters in two
chained steps; DuckDB uses an explicit CREATE VIEW. Same logical query.
"""


def run_spark(spark, table):
    spark.table(table).createOrReplaceTempView("_lkh08_sales")
    return spark.sql(
        "SELECT cat, SUM(amt) AS total FROM _lkh08_sales "
        "WHERE region = 'r1' AND amt > 50 GROUP BY cat"
    )


def run_duckdb(con, parquet_glob):
    con.execute(
        f"CREATE OR REPLACE VIEW _lkh08_v AS "
        f"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)"
    )
    return con.execute(
        "SELECT cat, SUM(amt) AS total FROM _lkh08_v "
        "WHERE region = 'r1' AND amt > 50 GROUP BY cat"
    ).arrow()
