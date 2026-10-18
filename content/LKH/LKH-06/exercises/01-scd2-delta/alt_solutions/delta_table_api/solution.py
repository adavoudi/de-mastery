"""LKH-06 / 01-scd2-delta — alt solution 1 (Python DeltaTable merge API).

Genuinely different from the reference: uses the programmatic
``delta.tables.DeltaTable.merge(...)`` builder instead of SQL MERGE text.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def scd2_merge(spark, table, updates_view):
    upd = spark.table(updates_view)

    # 1. Close changed current rows.
    DeltaTable.forName(spark, table).alias("t").merge(
        upd.alias("u"), "t.bkey = u.bkey AND t.valid_to IS NULL"
    ).whenMatchedUpdate(
        condition="t.attr <> u.attr", set={"valid_to": "u.eff"}
    ).execute()

    # 2. Insert new current versions for changed/new keys.
    DeltaTable.forName(spark, table).alias("t").merge(
        upd.alias("u"),
        "t.bkey = u.bkey AND t.valid_to IS NULL AND t.attr = u.attr",
    ).whenNotMatchedInsert(
        values={
            "bkey": "u.bkey",
            "attr": "u.attr",
            "valid_from": "u.eff",
            "valid_to": F.lit(None),
        }
    ).execute()
