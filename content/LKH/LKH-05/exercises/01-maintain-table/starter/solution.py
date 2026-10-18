"""LKH-05 / 01-maintain-table — starter. See README.md.

Implement maintain() to compact, sort, and expire old snapshots of an Iceberg
table WITHOUT changing its data.
"""


def maintain(spark, table, sort_col):
    # TODO:
    #   1. ALTER TABLE <table> WRITE ORDERED BY <sort_col>
    #   2. CALL local.system.rewrite_data_files(...)
    #   3. CALL local.system.expire_snapshots(... older_than ...)
    raise NotImplementedError
