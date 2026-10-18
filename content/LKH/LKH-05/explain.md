# LKH-05 · Explain it back (Feynman)

**Prompt A.** "A streaming pipeline has turned our Iceberg events table into 40,000 tiny files and
queries are crawling. Walk me through the maintenance job you'd schedule — in order — and say what
each step does, what it does NOT do, and how you keep time travel safe."

**Prompt B.** "Compare this to how you kept a Redshift table healthy with VACUUM and SORTKEY, and
explain why partitioning on a high-cardinality column would make the small-files problem worse, not
better."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names **compaction** (`rewrite_data_files`) as the fix for small files, and says it writes a
      new snapshot without losing data (old files stay until expired).
- [ ] Explains **sort order** (`WRITE ORDERED BY`) clusters rows so per-file min/max enable file
      pruning — the Redshift `SORTKEY` analogue.
- [ ] Explains **expire_snapshots** reclaims disk by dropping old snapshots and their files, and
      that `retain_last` alone won't touch recent snapshots because of `max_snapshot_age` — you need
      `older_than`.
- [ ] States that expiration must respect the **time-travel / rollback window** (don't expire your
      safety net).
- [ ] Distinguishes **orphan files** (unreferenced, from aborted writes) and that only
      `remove_orphan_files` reclaims them, with a conservative `older_than`.
- [ ] Explains why **high-cardinality partitioning** creates one tiny file per value (makes the
      problem worse); low-cardinality transform + sort order is the right split.
- [ ] (Bonus) Mentions **partition evolution** / hidden partitioning as metadata-only changes that
      avoid full rewrites, unlike Hive where the path encodes partitioning.
