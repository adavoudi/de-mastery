# LKH-06 · Explain it back (Feynman)

**Prompt A.** "You wrote an SCD2 dimension in Delta with `MERGE INTO`, enabled Change Data Feed,
and inspected `_delta_log`. Walk me through exactly what happens on disk and in the log when one
customer's tier changes: which rows change, what the commit files contain, and what CDF emits."

**Prompt B.** "A teammate says 'Delta and Iceberg are the same thing with different names.' Agree
and disagree: name two things they genuinely share and two places the metadata/APIs actually differ
(log fold + checkpoints vs. catalog pointer + manifests; CDF vs. changelog scan)."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes SCD2 correctly: **close** the current row (`valid_to = effective_date`) and
      **insert** a new current row (`valid_to NULL`); never overwrite the attribute in place.
- [ ] Explains the `_delta_log` is **ordered JSON commits** of add/remove actions, folded to get
      current state.
- [ ] Explains **checkpoints** (periodic Parquet) exist so readers don't replay every commit.
- [ ] Explains **CDF** emits `insert` / `update_preimage` / `update_postimage` / `delete` with
      `_commit_version`, and that it must be enabled *before* the changes.
- [ ] Contrasts with Iceberg: catalog pointer → one `metadata.json` + manifest list, vs. Delta's
      log fold; and names the shared concepts (ACID, optimistic concurrency, open Parquet).
- [ ] Identifies the SCD **Type 1** anti-pattern (overwrite) as the common bug.
- [ ] (Bonus) Mentions liquid clustering ≈ Iceberg sort order + hidden partitioning, and UniForm for
      cross-format reads.
