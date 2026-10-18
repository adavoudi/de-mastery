"""Mutant: compacts and expires but never sets a sort order, so the rewritten
file is not clustered by sort_col (no file pruning benefit)."""

import datetime


def _relative(table):
    parts = table.split(".")
    return ".".join(parts[1:]) if parts[0] == "local" else table


def maintain(spark, table, sort_col):
    rel = _relative(table)
    # WRONG: no ALTER TABLE ... WRITE ORDERED BY — files are not sorted.
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
