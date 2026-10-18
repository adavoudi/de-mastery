"""LKH-04 / 01-scd2-merge — alt: step 2 via a NOT IN subquery.

Same close step, but the new-version insert selects source keys that are not among
the still-current keys using NOT IN instead of a LEFT ANTI JOIN. Equivalent set.
"""


def apply_scd2(spark, dim_ident, src_values_sql, batch_date):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW _scd2_src AS {src_values_sql}")

    spark.sql(
        f"""
        MERGE INTO {dim_ident} t
        USING _scd2_src u
          ON t.nk = u.nk AND t.is_current = true
        WHEN MATCHED AND t.attr <> u.attr THEN
          UPDATE SET t.valid_to = DATE'{batch_date}', t.is_current = false
        """
    )

    spark.sql(
        f"""
        INSERT INTO {dim_ident}
        SELECT u.nk, u.attr, DATE'{batch_date}', CAST(NULL AS DATE), true
        FROM _scd2_src u
        WHERE u.nk NOT IN (SELECT nk FROM {dim_ident} WHERE is_current = true)
        """
    )
