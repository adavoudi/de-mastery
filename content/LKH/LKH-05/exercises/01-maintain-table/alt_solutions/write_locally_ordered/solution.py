"""LKH-05 / 01-maintain-table — alt solution 1 (WRITE LOCALLY ORDERED BY).

Genuinely different from the reference: declares a *local* sort order (sort within
each file/task rather than a global shuffle) and computes the expire cutoff from a
naive now() rather than a timedelta. Same end state: compacted, sorted, expired.
"""

import datetime


def _relative(table):
    parts = table.split(".")
    return ".".join(parts[1:]) if parts[0] == "local" else table


def maintain(spark, table, sort_col):
    rel = _relative(table)
    spark.sql(f"ALTER TABLE {table} WRITE LOCALLY ORDERED BY {sort_col}")
    spark.sql(
        f"CALL local.system.rewrite_data_files("
        f"table => '{rel}', options => map('min-input-files','2'))"
    )
    cutoff = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(
        hours=12
    )
    spark.sql(
        f"CALL local.system.expire_snapshots("
        f"table => '{rel}', older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}', "
        f"retain_last => 1)"
    )
