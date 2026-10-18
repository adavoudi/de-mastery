"""LKH-03 / 01-wap-branch — alt: publish via cherrypick_snapshot.

Same write-audit flow, but publish by cherry-picking the branch's snapshot onto
main instead of fast-forwarding. Equivalent result for a non-diverged main.
"""


def _db_table(table_ident):
    parts = table_ident.split(".")
    return ".".join(parts[1:]) if len(parts) == 3 else table_ident


def write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit"):
    spark.sql(f"ALTER TABLE {table_ident} CREATE OR REPLACE BRANCH {branch}")
    spark.sql(f"INSERT INTO {table_ident}.branch_{branch} {insert_values_sql}")

    branch_df = spark.sql(f"SELECT * FROM {table_ident}.branch_{branch}")
    branch_count = branch_df.count()
    passed = bool(audit_fn(branch_df))

    if passed:
        snap = spark.sql(
            f"SELECT snapshot_id FROM {table_ident}.refs WHERE name = '{branch}'"
        ).collect()[0]["snapshot_id"]
        spark.sql(
            f"CALL local.system.cherrypick_snapshot('{_db_table(table_ident)}', {snap})"
        )

    spark.sql(f"ALTER TABLE {table_ident} DROP BRANCH {branch}")
    main_count = spark.sql(f"SELECT count(*) AS c FROM {table_ident}").collect()[0]["c"]
    return {"published": passed, "main_count": main_count, "branch_count": branch_count}
