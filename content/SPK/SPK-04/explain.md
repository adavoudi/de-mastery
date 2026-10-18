# SPK-04 · Explain it back (Feynman)

**Prompt A.** "I come from dbt. Map my mental model onto Spark SQL. What is a `ref()`? What is a
`materialized='table'` model? What is `--full-refresh`? How do I query a file that I have not
registered? Where does the analogy break?"

**Prompt B.** "The revenue of a nightly refresh job doubled overnight, with no new orders. Walk
me through the most likely data-source bug. Show how save modes cause it."

## Rubric — a strong explanation covers these (5–7):

- [ ] temp view = a session-scoped name on a lazy plan (≈ `ref()`). It stores no bytes.
- [ ] persistent/managed table (`saveAsTable`) ≈ `materialized='table'`. Says who owns the files on DROP.
- [ ] `SELECT FROM parquet.\`path\`` for SQL on files (≈ Athena on a prefix).
- [ ] save modes: `overwrite` ≈ `--full-refresh`, `append` ≈ incremental add. Names the
      append-instead-of-overwrite doubling bug.
- [ ] explicit schema on CSV/JSON instead of `inferSchema`, and why (extra scan, wrong types).
- [ ] `partitionBy` on a low-cardinality column → partition pruning. On a high-cardinality
      column → tiny files.
- [ ] (Bonus) SQL and DataFrame compile to the same Catalyst plan.
