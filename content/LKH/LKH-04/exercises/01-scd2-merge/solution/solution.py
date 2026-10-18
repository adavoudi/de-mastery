"""LKH-04 / 01-scd2-merge — reference solution (two-step MERGE + INSERT)."""


def apply_scd2(spark, dim_ident, src_values_sql, batch_date):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW _scd2_src AS {src_values_sql}")

    # Step 1: close current rows whose attribute changed.
    spark.sql(
        f"""
        MERGE INTO {dim_ident} t
        USING _scd2_src u
          ON t.nk = u.nk AND t.is_current = true
        WHEN MATCHED AND t.attr <> u.attr THEN
          UPDATE SET t.valid_to = DATE'{batch_date}', t.is_current = false
        """
    )

    # Step 2: insert a new open version for changed + brand-new keys.
    spark.sql(
        f"""
        INSERT INTO {dim_ident}
        SELECT u.nk, u.attr, DATE'{batch_date}', CAST(NULL AS DATE), true
        FROM _scd2_src u
        LEFT ANTI JOIN (SELECT nk FROM {dim_ident} WHERE is_current = true) c
          ON u.nk = c.nk
        """
    )
