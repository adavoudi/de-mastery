"""LKH-06 / 01-scd2-delta — alt solution 2 (single MERGE, union-staged source).

Genuinely different from the reference: the classic single-statement SCD2 pattern.
The source unions each update twice — once with its real key (to CLOSE the current
row) and, for changed keys only, once with a NULL mergeKey (to force a NOT MATCHED
INSERT of the new version). One MERGE then does both.
"""


def scd2_merge(spark, table, updates_view):
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW _scd_staged AS
        SELECT u.bkey AS mergeKey, u.bkey, u.attr, u.eff
        FROM {updates_view} u
        UNION ALL
        SELECT NULL AS mergeKey, u.bkey, u.attr, u.eff
        FROM {updates_view} u
        JOIN {table} t ON t.bkey = u.bkey AND t.valid_to IS NULL
        WHERE t.attr <> u.attr
        """
    )
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING _scd_staged s
          ON t.bkey = s.mergeKey AND t.valid_to IS NULL
        WHEN MATCHED AND t.attr <> s.attr
          THEN UPDATE SET t.valid_to = s.eff
        WHEN NOT MATCHED
          THEN INSERT (bkey, attr, valid_from, valid_to)
               VALUES (s.bkey, s.attr, s.eff, NULL)
        """
    )
