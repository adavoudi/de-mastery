"""Mutant: expires snapshots but never compacts, so the many small files remain."""

import datetime


def _relative(table):
    parts = table.split(".")
    return ".".join(parts[1:]) if parts[0] == "local" else table


def maintain(spark, table, sort_col):
    rel = _relative(table)
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {sort_col}")
    # WRONG: no rewrite_data_files — the small files are never combined.
    cutoff = (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    spark.sql(
        f"CALL local.system.expire_snapshots("
        f"table => '{rel}', older_than => TIMESTAMP '{cutoff}', retain_last => 1)"
    )
