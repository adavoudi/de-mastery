"""Mutant: inserts new versions but never closes the old current row, leaving two
open (valid_to IS NULL) rows for every changed key."""


def scd2_merge(spark, table, updates_view):
    # WRONG: no closing MERGE. The old current row stays open AND a new current row
    # is inserted, so changed keys end up with two open versions.
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {updates_view} u
          ON t.bkey = u.bkey AND t.valid_to IS NULL AND t.attr = u.attr
        WHEN NOT MATCHED
          THEN INSERT (bkey, attr, valid_from, valid_to)
               VALUES (u.bkey, u.attr, u.eff, NULL)
        """
    )
