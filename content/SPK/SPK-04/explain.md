# SPK-04 · Explain it back (Feynman)

**Prompt A.** "I come from dbt. Map my mental model onto Spark SQL: what's a `ref()`, what's a
`materialized='table'` model, what's `--full-refresh`, and how do I query a file I haven't
registered? Where does the analogy break?"

**Prompt B.** "A nightly refresh job's revenue doubled overnight with no new orders. Walk me
through the most likely data-source bug and how save modes cause it."

## Rubric — a strong explanation hits these (5–7):

- [ ] temp view = a session-scoped name on a lazy plan (≈ `ref()`), stores no bytes.
- [ ] persistent/managed table (`saveAsTable`) ≈ `materialized='table'`; who owns the files on DROP.
- [ ] `SELECT FROM parquet.\`path\`` for SQL on files (≈ Athena on a prefix).
- [ ] save modes: `overwrite` ≈ `--full-refresh`, `append` ≈ incremental add; names the
      append-instead-of-overwrite doubling bug.
- [ ] explicit schema on CSV/JSON over `inferSchema` and why (extra scan, wrong types).
- [ ] `partitionBy` low-cardinality column → partition pruning; high cardinality → tiny files.
- [ ] (Bonus) SQL and DataFrame compile to the same Catalyst plan.
