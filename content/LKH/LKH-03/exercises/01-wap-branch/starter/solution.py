"""LKH-03 / 01-wap-branch — starter. See README.md."""


def write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit"):
    # TODO: create the staging branch, write candidate rows to the BRANCH, audit the
    # branch's rows with audit_fn, fast-forward main ONLY if the audit passes, drop the
    # branch, and return {"published": ..., "main_count": ..., "branch_count": ...}.
    raise NotImplementedError
