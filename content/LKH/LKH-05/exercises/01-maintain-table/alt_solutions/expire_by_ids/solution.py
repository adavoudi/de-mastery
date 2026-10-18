"""LKH-05 / 01-maintain-table — alt solution 2 (expire by explicit snapshot_ids).

Genuinely different from the reference: after compacting, it reads the snapshots
metadata table to find the newest snapshot and expires every OTHER snapshot by id
(``snapshot_ids => array(...)``) instead of using an ``older_than`` time cutoff.
"""


def _relative(table):
    parts = table.split(".")
    return ".".join(parts[1:]) if parts[0] == "local" else table


def maintain(spark, table, sort_col):
    rel = _relative(table)
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {sort_col}")
    spark.sql(
        f"CALL local.system.rewrite_data_files("
        f"table => '{rel}', options => map('min-input-files','2'))"
    )
    snaps = [
        r["snapshot_id"]
        for r in spark.sql(
            f"SELECT snapshot_id, committed_at FROM {table}.snapshots "
            f"ORDER BY committed_at"
        ).collect()
    ]
    old = snaps[:-1]  # keep the newest (compacted) snapshot
    if old:
        ids = ",".join(str(s) for s in old)
        spark.sql(
            f"CALL local.system.expire_snapshots("
            f"table => '{rel}', snapshot_ids => array({ids}))"
        )
