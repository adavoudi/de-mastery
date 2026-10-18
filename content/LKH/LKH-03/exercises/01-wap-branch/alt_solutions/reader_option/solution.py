"""LKH-03 / 01-wap-branch — alt: audit via the DataFrameReader branch option.

Reads the branch snapshot with spark.read.option("branch", ...).table(...) instead
of the SQL branch_<name> syntax, then publishes with fast_forward. Different read
surface, same guarantee.
"""


def _db_table(table_ident):
    parts = table_ident.split(".")
    return ".".join(parts[1:]) if len(parts) == 3 else table_ident


def write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit"):
    spark.sql(f"ALTER TABLE {table_ident} CREATE OR REPLACE BRANCH {branch}")
    spark.sql(f"INSERT INTO {table_ident}.branch_{branch} {insert_values_sql}")

    branch_df = spark.read.option("branch", branch).table(table_ident)
    branch_count = branch_df.count()
    passed = bool(audit_fn(branch_df))

    if passed:
        spark.sql(
            f"CALL local.system.fast_forward('{_db_table(table_ident)}', 'main', '{branch}')"
        )

    spark.sql(f"ALTER TABLE {table_ident} DROP BRANCH {branch}")
    main_count = spark.read.table(table_ident).count()
    return {"published": passed, "main_count": main_count, "branch_count": branch_count}
