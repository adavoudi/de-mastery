"""Mutant: writes straight to main, skipping the branch (no isolation)."""


def _db_table(table_ident):
    parts = table_ident.split(".")
    return ".".join(parts[1:]) if len(parts) == 3 else table_ident


def write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit"):
    # WRONG: insert into main directly; bad data is visible before any audit.
    spark.sql(f"INSERT INTO {table_ident} {insert_values_sql}")
    df = spark.sql(f"SELECT * FROM {table_ident}")
    branch_count = df.count()
    passed = bool(audit_fn(df))
    main_count = spark.sql(f"SELECT count(*) AS c FROM {table_ident}").collect()[0]["c"]
    return {"published": passed, "main_count": main_count, "branch_count": branch_count}
