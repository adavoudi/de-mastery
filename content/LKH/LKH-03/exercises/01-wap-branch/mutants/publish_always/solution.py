"""Mutant: publishes unconditionally, ignoring the audit result."""


def _db_table(table_ident):
    parts = table_ident.split(".")
    return ".".join(parts[1:]) if len(parts) == 3 else table_ident


def write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit"):
    spark.sql(f"ALTER TABLE {table_ident} CREATE OR REPLACE BRANCH {branch}")
    spark.sql(f"INSERT INTO {table_ident}.branch_{branch} {insert_values_sql}")
    branch_df = spark.sql(f"SELECT * FROM {table_ident}.branch_{branch}")
    branch_count = branch_df.count()
    audit_fn(branch_df)  # result computed but IGNORED
    # WRONG: fast-forward no matter what the audit said.
    spark.sql(
        f"CALL local.system.fast_forward('{_db_table(table_ident)}', 'main', '{branch}')"
    )
    spark.sql(f"ALTER TABLE {table_ident} DROP BRANCH {branch}")
    main_count = spark.sql(f"SELECT count(*) AS c FROM {table_ident}").collect()[0]["c"]
    return {"published": True, "main_count": main_count, "branch_count": branch_count}
