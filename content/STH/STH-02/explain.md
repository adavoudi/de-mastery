# STH-02 · Explain it back (Feynman)

**Prompt A.** "Explain how a Paimon primary-key table merges rows that share a key. Explain why
`partial-update` is the right merge engine to build a customer golden record from three source
systems. Say exactly what would go wrong if you used the default `deduplicate` engine instead."

**Prompt B.** "Map a Paimon partial-update table onto the way you build an MDM golden record in dbt
today. Cover the primary key, the bucket and the merge engine. Where does the storage-layer version
help? Where is a plain Iceberg table still the better choice?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Paimon is an LSM tree on the lake. PK tables merge rows with the same key via a merge engine.
- [ ] `partial-update` keeps the latest **non-null** value **per column**. So partial rows from
      different writers combine into one complete row.
- [ ] The default `deduplicate` keeps the last whole record (last-writer-wins). It would overwrite
      the columns of earlier systems with NULLs. This is MDM data loss.
- [ ] A PK table needs `PRIMARY KEY(...) NOT ENFORCED`. A small deterministic table needs
      `'bucket'='1'`.
- [ ] A later `NULL` does not erase an earlier non-null value under `partial-update`.
- [ ] You map PK = business key, bucket = physical layout, merge engine = coalesce/precedence
      policy. You say that this is the dbt golden-record model, done continuously in storage.
- [ ] You know when NOT to use it (immutable facts, true last-writer-wins, OLAP-only).
