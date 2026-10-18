"""Mutant: partitions on the high-cardinality id column before compacting, which
creates roughly one small file per id — the small-files problem by construction."""

import datetime


def _relative(table):
    parts = table.split(".")
    return ".".join(parts[1:]) if parts[0] == "local" else table


def maintain(spark, table, sort_col):
    rel = _relative(table)
    # WRONG: partition on a high-cardinality column -> one tiny file per id.
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD id")
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {sort_col}")
    spark.sql(
        f"CALL local.system.rewrite_data_files("
        f"table => '{rel}', options => map('min-input-files','2'))"
    )
    cutoff = (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    spark.sql(
        f"CALL local.system.expire_snapshots("
        f"table => '{rel}', older_than => TIMESTAMP '{cutoff}', retain_last => 1)"
    )
