"""Mutant: "compacts" by overwriting the table with a de-duplicated/filtered copy,
which silently drops rows — compaction must NEVER change the data."""

import datetime


def _relative(table):
    parts = table.split(".")
    return ".".join(parts[1:]) if parts[0] == "local" else table


def maintain(spark, table, sort_col):
    rel = _relative(table)
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {sort_col}")
    # WRONG: rewrites the data through a DISTINCT on sort_col, collapsing rows that
    # share an event_date. This "fixes" file count by destroying data.
    df = spark.sql(f"SELECT * FROM {table}").dropDuplicates([sort_col])
    df.writeTo(table).replace()
    cutoff = (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    spark.sql(
        f"CALL local.system.expire_snapshots("
        f"table => '{rel}', older_than => TIMESTAMP '{cutoff}', retain_last => 1)"
    )
