# Write-audit-publish with an Iceberg branch

Model the SOX "audited publish" control with an Iceberg branch. New data must pass quality checks
on a staging branch before it can reach `main`. Implement this in `solution.py`:

## `write_audit_publish(spark, table_ident, insert_values_sql, audit_fn, branch="audit") -> dict`

- `spark` — the Iceberg-enabled SparkSession (catalog `local`).
- `table_ident` — the full table name, for example `local.db.orders`.
- `insert_values_sql` — a SQL `VALUES (...), (...)` (or `SELECT ...`) clause for the candidate rows.
- `audit_fn(df) -> bool` — your quality gate. It receives a DataFrame of the **branch's** rows. It
  returns `True` to allow publishing and `False` to block.
- `branch` — the name of the staging branch.

Steps (order matters):

1. **Write.** Create the staging branch (`ALTER TABLE ... CREATE OR REPLACE BRANCH <branch>`). Insert
   the candidate rows into it: `INSERT INTO <table>.branch_<branch> <insert_values_sql>`. Do
   **not** insert into `<table>` directly. That is `main`.
2. **Audit.** Read the rows of the branch (`SELECT * FROM <table>.branch_<branch>`). Call
   `audit_fn(that_df)`.
3. **Publish only if the audit passed.** Fast-forward main to the branch:
   `CALL local.system.fast_forward('<db.table>', 'main', '<branch>')`. Here, `<db.table>` is
   `table_ident` without the `local.` catalog prefix. If the audit failed, do **not** publish.
4. Always drop the staging branch at the end (`ALTER TABLE ... DROP BRANCH <branch>`).

Return a dict:

| key | value |
|---|---|
| `published` | `True` if you fast-forwarded main, else `False` |
| `main_count` | row count of `main` **after** the flow |
| `branch_count` | row count the branch held during the audit |

Rules and traps:
- **Write to the branch, never to main.** If you write to main, you lose the isolation.
- **Audit the snapshot of the branch**, not main. Main does not contain the candidate rows yet.
- **Publish only after a passing audit.** If you publish first (or unconditionally), bad data gets
  in.

The tests supply a good batch (audit passes, so main grows). They also supply a bad batch (audit
fails, so main is unchanged). The tests compare the state of main with an independent expectation.
