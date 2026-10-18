"""Mutant: audits MAIN instead of the branch (the gate validates the wrong data).

Because main does not yet contain the candidate rows, the audit sees only the old,
already-published rows and (wrongly) passes, so bad branch data is published.
"""


def _db_table(table_ident):
    parts = table_ident.split(".")
    return ".".join(parts[1:]) if len(parts) == 3 else table_ident


def write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit"):
    spark.sql(f"ALTER TABLE {table_ident} CREATE OR REPLACE BRANCH {branch}")
    spark.sql(f"INSERT INTO {table_ident}.branch_{branch} {insert_values_sql}")

    # WRONG: audit main, which lacks the candidate rows, instead of the branch.
    main_df = spark.sql(f"SELECT * FROM {table_ident}")
    branch_count = spark.sql(
        f"SELECT count(*) AS c FROM {table_ident}.branch_{branch}"
    ).collect()[0]["c"]
    passed = bool(audit_fn(main_df))

    if passed:
        spark.sql(
            f"CALL local.system.fast_forward('{_db_table(table_ident)}', 'main', '{branch}')"
        )
    spark.sql(f"ALTER TABLE {table_ident} DROP BRANCH {branch}")
    main_count = spark.sql(f"SELECT count(*) AS c FROM {table_ident}").collect()[0]["c"]
    return {"published": passed, "main_count": main_count, "branch_count": branch_count}
