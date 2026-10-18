"""LKH-03 / 01-wap-branch — reference solution.

Write to a staging branch, audit the branch's snapshot, and fast-forward main only
on a passing audit. main readers never see unaudited data.
"""


def _db_table(table_ident):
    # fast_forward wants the identifier WITHOUT the catalog prefix, e.g. 'db.orders'.
    parts = table_ident.split(".")
    return ".".join(parts[1:]) if len(parts) == 3 else table_ident


def write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit"):
    # 1. WRITE to the branch (main is untouched).
    spark.sql(f"ALTER TABLE {table_ident} CREATE OR REPLACE BRANCH {branch}")
    spark.sql(f"INSERT INTO {table_ident}.branch_{branch} {insert_values_sql}")

    # 2. AUDIT the branch's rows.
    branch_df = spark.sql(f"SELECT * FROM {table_ident}.branch_{branch}")
    branch_count = branch_df.count()
    passed = bool(audit_fn(branch_df))

    # 3. PUBLISH only on pass.
    if passed:
        spark.sql(
            f"CALL local.system.fast_forward('{_db_table(table_ident)}', 'main', '{branch}')"
        )

    # 4. Always drop the staging branch.
    spark.sql(f"ALTER TABLE {table_ident} DROP BRANCH {branch}")

    main_count = spark.sql(f"SELECT count(*) AS c FROM {table_ident}").collect()[0]["c"]
    return {"published": passed, "main_count": main_count, "branch_count": branch_count}
