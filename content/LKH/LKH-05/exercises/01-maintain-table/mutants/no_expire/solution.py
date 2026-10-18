"""Mutant: compacts and sorts but never expires snapshots, so every per-batch
snapshot (and the data files it pins) lingers — disk is never reclaimed."""


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
    # WRONG: no expire_snapshots — old snapshots are never reclaimed.
