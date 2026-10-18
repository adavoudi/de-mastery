"""Mutant: overwrites the attribute in place (SCD Type 1) — destroys history."""


def scd2_merge(spark, table, updates_view):
    # WRONG: UPDATE SET t.attr overwrites the current row instead of versioning it.
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {updates_view} u
          ON t.bkey = u.bkey AND t.valid_to IS NULL
        WHEN MATCHED AND t.attr <> u.attr
          THEN UPDATE SET t.attr = u.attr
        WHEN NOT MATCHED
          THEN INSERT (bkey, attr, valid_from, valid_to)
               VALUES (u.bkey, u.attr, u.eff, NULL)
        """
    )
