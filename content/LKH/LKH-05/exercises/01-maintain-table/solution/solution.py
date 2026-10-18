"""LKH-05 / 01-maintain-table — reference solution.

Recipe: set the sort order, compact, then expire old snapshots with an
``older_than`` cutoff. Data is never filtered or transformed, so it is unchanged.
"""

import datetime


def _relative(table):
    # 'local.lkh05.events' -> 'lkh05.events' (the name a CALL procedure expects).
    parts = table.split(".")
    return ".".join(parts[1:]) if parts[0] == "local" else table


def maintain(spark, table, sort_col):
    rel = _relative(table)

    # 1. Sort order so compaction clusters files by sort_col (feeds file pruning).
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {sort_col}")

    # 2. Compact many small files into few larger ones (new snapshot, data intact).
    spark.sql(
        f"CALL local.system.rewrite_data_files("
        f"table => '{rel}', options => map('min-input-files','2'))"
    )

    # 3. Expire old snapshots. retain_last alone is blocked by max_snapshot_age,
    #    so pass a near-future older_than cutoff; keep only the final snapshot.
    cutoff = (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    spark.sql(
        f"CALL local.system.expire_snapshots("
        f"table => '{rel}', older_than => TIMESTAMP '{cutoff}', retain_last => 1)"
    )
