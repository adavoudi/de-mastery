"""Mutant: SCD1 — overwrites the attribute in place, no new version (history lost)."""


def apply_scd2(spark, dim_ident, src_values_sql, batch_date):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW _scd2_src AS {src_values_sql}")
    # WRONG: update the current row's attr in place; never version it.
    spark.sql(
        f"""
        MERGE INTO {dim_ident} t
        USING _scd2_src u
          ON t.nk = u.nk AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET t.attr = u.attr
        WHEN NOT MATCHED THEN
          INSERT (nk, attr, valid_from, valid_to, is_current)
          VALUES (u.nk, u.attr, DATE'{batch_date}', CAST(NULL AS DATE), true)
        """
    )
