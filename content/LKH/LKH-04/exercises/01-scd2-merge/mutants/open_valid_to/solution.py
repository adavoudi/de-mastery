"""Mutant: closes is_current but leaves valid_to NULL (two 'open' rows per key)."""


def apply_scd2(spark, dim_ident, src_values_sql, batch_date):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW _scd2_src AS {src_values_sql}")
    spark.sql(
        f"""
        MERGE INTO {dim_ident} t
        USING _scd2_src u
          ON t.nk = u.nk AND t.is_current = true
        WHEN MATCHED AND t.attr <> u.attr THEN
          UPDATE SET t.is_current = false
        """
    )  # WRONG: valid_to never set, so the superseded row stays 'open'
    spark.sql(
        f"""
        INSERT INTO {dim_ident}
        SELECT u.nk, u.attr, DATE'{batch_date}', CAST(NULL AS DATE), true
        FROM _scd2_src u
        LEFT ANTI JOIN (SELECT nk FROM {dim_ident} WHERE is_current = true) c
          ON u.nk = c.nk
        """
    )
