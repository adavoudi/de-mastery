"""Mutant: closes changed rows but never inserts the new versions, so changed keys
lose their current row and brand-new keys never appear."""


def scd2_merge(spark, table, updates_view):
    # WRONG: only the closing MERGE; no insert of the new current versions.
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {updates_view} u
          ON t.bkey = u.bkey AND t.valid_to IS NULL
        WHEN MATCHED AND t.attr <> u.attr
          THEN UPDATE SET t.valid_to = u.eff
        """
    )
