# LKH-03 · Catalogs, branching & write-audit-publish

## Why the catalog is the real boundary

LKH-01 showed that a table is a tree of files; LKH-02 showed that a commit is an atomic swap of one
pointer. The thing that **holds** that pointer — and decides who may swap it — is the **catalog**.
In your Athena world the catalog is **Glue**: it maps `db.table` to the current `metadata.json` and
(with Lake Formation) governs who can read or write. Swap the catalog and the same Iceberg files
become a Hive-metastore table, a REST-catalog table (Polaris, Nessie, Unity, Lakekeeper), or a
plain filesystem table. The data is portable; the catalog is the control plane.

A catalog does three jobs: **name resolution** (`db.table` → metadata path), **atomic commit** (the
compare-and-swap of LKH-02), and increasingly **governance** (permissions, lineage). The REST
catalog standardises this behind one HTTP protocol so any engine talks to any catalog the same way.

## Branching, tagging, and write-audit-publish

Iceberg can keep multiple named **references** to snapshots in one table:

- A **tag** is an immutable label on a single snapshot — a named point in time ("eom-2026-01").
- A **branch** is a mutable reference you can commit to independently of `main`, exactly like a git
  branch. Writes to a branch do **not** change what readers of `main` see.

This is the mechanism behind **write-audit-publish (WAP)** — the pattern that matches your SOX
"audited publish" control. Instead of writing new data straight into the production table (`main`)
and hoping it's good, you:

1. **Write** the new data to a short-lived **branch** (e.g. `audit`). Production readers on `main`
   are untouched; nothing bad is visible yet.
2. **Audit** the data *on the branch* — run your quality checks (row counts, null rates, referential
   integrity, control totals) against the branch's snapshot, not against main.
3. **Publish** only if the audit passes: **fast-forward** `main` to the branch tip (when main hasn't
   moved) or **cherry-pick** the branch's snapshot onto main. If the audit fails, you drop the branch
   and `main` never saw the bad data.

The order is the whole point. Auditing *before* publishing is what makes the control real; auditing
the wrong snapshot (main instead of the branch), or publishing before the audit, defeats it — the
same way signing off a SOX control on last quarter's numbers, or publishing before sign-off, would.

![Writes land on the audit branch; main only moves, by fast-forward, after the audit of S4 passes](diagrams/wap-branch-lineage.svg)

In Spark SQL (Iceberg extensions) the moves are:

```sql
ALTER TABLE t CREATE BRANCH audit;              -- create the staging branch
INSERT INTO t.branch_audit SELECT ... ;         -- write-audit-publish: WRITE
-- AUDIT: run checks against t.branch_audit (VERSION AS OF 'audit')
CALL cat.system.fast_forward('db.t', 'main', 'audit');   -- PUBLISH (ff)
-- or: CALL cat.system.cherrypick_snapshot('db.t', <snapshot_id>)
```

## From your world

| SOX / dbt / Athena | Iceberg | Note |
|---|---|---|
| Glue Data Catalog | the catalog (name → metadata, commit, governance) | one control plane |
| Lake Formation grants | catalog governance (Unity/Polaris/LF) | who may read/write |
| SOX "no unapproved data in the mart" | write to a branch, not main | isolation before sign-off |
| the audited publish / sign-off step | audit the branch, then fast-forward main | approval gate |
| dbt `--defer`/CI build on a PR | WAP branch validated before merge | same shape |

## When NOT to use branches / WAP

- **Don't branch for every tiny append** where a failed load is cheap to retry; WAP shines when a
  bad publish is expensive or regulated. Branch maintenance (and extra snapshots) isn't free.
- **Don't treat a branch as long-lived parallel history** unless you really need it; branches that
  live forever accumulate snapshots and complicate expiration (LKH-05).
- **Don't rely on branch isolation for secrecy/row-level security** — that's the catalog's
  governance job, not what a branch is for.

## Further reading

- Iceberg — Branching and Tagging:
  <https://iceberg.apache.org/docs/latest/branching/>
- Iceberg spec — Snapshot References (branches/tags):
  <https://iceberg.apache.org/spec/#snapshot-references>
- Iceberg Spark procedures (fast_forward, cherrypick_snapshot):
  <https://iceberg.apache.org/docs/latest/spark-procedures/>
