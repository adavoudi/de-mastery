"""LKH-06 / 01-scd2-delta — reference solution (two SQL MERGE statements)."""


def scd2_merge(spark, table, updates_view):
    # 1. Close the current version of every key whose attribute changed.
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {updates_view} u
          ON t.bkey = u.bkey AND t.valid_to IS NULL
        WHEN MATCHED AND t.attr <> u.attr
          THEN UPDATE SET t.valid_to = u.eff
        """
    )
    # 2. Insert a new current version for changed and brand-new keys. Requiring
    #    t.attr = u.attr in the ON means unchanged keys stay MATCHED (skipped),
    #    while changed/new keys are NOT MATCHED and get inserted.
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
