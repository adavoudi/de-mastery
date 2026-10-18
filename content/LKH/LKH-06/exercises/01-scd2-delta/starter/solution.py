"""LKH-06 / 01-scd2-delta — starter. See README.md.

Implement an SCD Type 2 upsert into a Delta table using MERGE: close changed
current rows, insert new versions, leave unchanged keys alone.
"""


def scd2_merge(spark, table, updates_view):
    # TODO:
    #   1. MERGE to close changed current rows (set valid_to = eff).
    #   2. MERGE to insert new current versions for changed/new keys.
    raise NotImplementedError
