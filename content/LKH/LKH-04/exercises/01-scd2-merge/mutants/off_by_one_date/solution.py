"""Mutant: off-by-one effective date — new valid_from = batch_date + 1 (a gap).

The old row is closed at batch_date but the new version starts the day AFTER, so a
key's validity has a one-day gap (old.valid_to != new.valid_from).
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
        SELECT u.nk, u.attr, date_add(DATE'{batch_date}', 1), CAST(NULL AS DATE), true
        FROM _scd2_src u
        LEFT ANTI JOIN (SELECT nk FROM {dim_ident} WHERE is_current = true) c
          ON u.nk = c.nk
        """
    )  # WRONG: valid_from is batch_date + 1 -> gap between old.valid_to and new.valid_from
