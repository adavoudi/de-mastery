# Write-audit-publish with an Iceberg branch

Model the SOX "audited publish" control with an Iceberg branch: new data must pass quality checks
on a staging branch before it is allowed to reach `main`. Implement in `solution.py`:

## `write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit") -> dict`

- `spark` — the Iceberg-enabled SparkSession (catalog `local`).
- `table_ident` — the full table name, e.g. `local.db.orders`.
- `insert_values_sql` — a SQL `VALUES (...), (...)` (or `SELECT ...`) clause for the candidate rows.
- `audit_fn(df) -> bool` — your quality gate; it receives a DataFrame of the **branch's** rows and
  returns `True` to allow publishing, `False` to block.
- `branch` — the staging branch name.

Steps (order matters):

1. **Write.** Create the staging branch (`ALTER TABLE ... CREATE OR REPLACE BRANCH <branch>`) and
   insert the candidate rows into it: `INSERT INTO <table>.branch_<branch> <insert_values_sql>`.
   Do **not** insert into `<table>` directly — that is `main`.
2. **Audit.** Read the branch's rows (`SELECT * FROM <table>.branch_<branch>`) and call
   `audit_fn(that_df)`.
3. **Publish only if the audit passed.** Fast-forward main to the branch:
   `CALL local.system.fast_forward('<db.table>', 'main', '<branch>')`, where `<db.table>` is
   `table_ident` without the `local.` catalog prefix. If the audit failed, do **not** publish.
4. Always drop the staging branch at the end (`ALTER TABLE ... DROP BRANCH <branch>`).

Return a dict:

| key | value |
|---|---|
| `published` | `True` if you fast-forwarded main, else `False` |
| `main_count` | row count of `main` **after** the flow |
| `branch_count` | row count the branch held during the audit |

Rules / traps:
- **Write to the branch, never to main.** Writing to main defeats isolation.
- **Audit the branch's snapshot**, not main — main doesn't contain the candidate rows yet.
- **Publish only after a passing audit.** Publishing first (or unconditionally) lets bad data in.

The tests supply a good batch (audit passes → main grows) and a bad batch (audit fails → main is
unchanged), and assert main's state against an independent expectation.
