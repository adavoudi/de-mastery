# STH-02 · Explain it back (Feynman)

**Prompt A.** "Explain how a Paimon primary-key table merges rows that share a key, and why
`partial-update` is the right merge engine for building a customer golden record from three source
systems. Say exactly what would go wrong if you used the default `deduplicate` engine instead."

**Prompt B.** "Map a Paimon partial-update table onto how you build an MDM golden record in dbt
today — primary key, bucket, merge engine. Where does the storage-layer version help, and where is
a plain Iceberg table still the better choice?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Paimon is an LSM tree on the lake; PK tables merge rows with the same key via a merge engine.
- [ ] `partial-update` keeps the latest **non-null** value **per column**, so partial rows from
      different writers combine into one complete row.
- [ ] The default `deduplicate` keeps the last whole record (last-writer-wins), which would
      overwrite earlier systems' columns with NULLs — MDM data loss.
- [ ] A PK table needs `PRIMARY KEY(...) NOT ENFORCED`; a small deterministic table needs
      `'bucket'='1'`.
- [ ] A later `NULL` does not erase an earlier non-null value under `partial-update`.
- [ ] Maps PK = business key, bucket = physical layout, merge engine = coalesce/precedence policy;
      the dbt golden-record model done continuously in storage.
- [ ] Knows when NOT to use it (immutable facts, true last-writer-wins, OLAP-only).
