# LKH-05 · Explain it back (Feynman)

**Prompt A.** "A streaming pipeline has turned our Iceberg events table into 40,000 tiny files.
Queries are very slow. Explain the maintenance job that you would schedule, in order. For each
step, say what it does, what it does NOT do, and how you keep time travel safe."

**Prompt B.** "Compare this to how you kept a Redshift table healthy with VACUUM and SORTKEY.
Explain why partitioning on a high-cardinality column makes the small-files problem worse, not
better."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names **compaction** (`rewrite_data_files`) as the fix for small files. Says that it writes
      a new snapshot and loses no data (old files stay until you expire them).
- [ ] Explains that **sort order** (`WRITE ORDERED BY`) clusters rows, so per-file min/max enable
      file pruning. This is the equivalent of the Redshift `SORTKEY`.
- [ ] Explains that **expire_snapshots** reclaims disk by dropping old snapshots and their files.
      Explains that `retain_last` alone does not touch recent snapshots because of
      `max_snapshot_age`. You need `older_than`.
- [ ] States that expiration must respect the **time-travel / rollback window** (do not expire
      your safety net).
- [ ] Distinguishes **orphan files** (unreferenced, from aborted writes). States that only
      `remove_orphan_files` reclaims them, with a conservative `older_than`.
- [ ] Explains why **high-cardinality partitioning** creates one tiny file per value (it makes the
      problem worse). The right split is a low-cardinality transform plus a sort order.
- [ ] (Bonus) Mentions **partition evolution** and hidden partitioning. They are metadata-only
      changes that avoid full rewrites. In Hive, the path encodes the partitioning.
