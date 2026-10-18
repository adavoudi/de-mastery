# LKH-06 · Explain it back (Feynman)

**Prompt A.** "You wrote an SCD2 dimension in Delta with `MERGE INTO`. You enabled Change Data
Feed and inspected `_delta_log`. Explain exactly what happens on disk and in the log when the tier
of one customer changes. Say which rows change, what the commit files contain, and what CDF
emits."

**Prompt B.** "A teammate says 'Delta and Iceberg are the same thing with different names.' Agree
and disagree. Name two things that they really share. Name two places where the metadata or APIs
differ (log fold and checkpoints vs. catalog pointer and manifests, CDF vs. changelog scan)."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes SCD2 correctly: **close** the current row (`valid_to = effective_date`) and
      **insert** a new current row (`valid_to NULL`). Never overwrite the attribute in place.
- [ ] Explains that the `_delta_log` holds **ordered JSON commits** of add/remove actions. A fold
      of these commits gives the current state.
- [ ] Explains that **checkpoints** (periodic Parquet) exist so that readers do not replay every
      commit.
- [ ] Explains that **CDF** emits `insert` / `update_preimage` / `update_postimage` / `delete`
      with `_commit_version`. Explains that you must enable it *before* the changes.
- [ ] Contrasts with Iceberg: a catalog pointer to one `metadata.json` + manifest list, vs. the log
      fold of Delta. Names the shared concepts (ACID, optimistic concurrency, open Parquet).
- [ ] Identifies the SCD **Type 1** anti-pattern (overwrite) as the common bug.
- [ ] (Bonus) Mentions that liquid clustering is about the same as Iceberg sort order + hidden
      partitioning. Mentions UniForm for cross-format reads.
