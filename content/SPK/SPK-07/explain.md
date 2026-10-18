# SPK-07 · Explain it back (Feynman)

**Prompt A.** "I have a pipeline that writes 5,000 tiny Parquet files and the downstream Athena-style
query is slow. Walk me through why that hurts, and how you'd hit a target file *count/size* — and be
precise about `repartition` vs `coalesce` vs `partitionBy`."

**Prompt B.** "Two large tables are joined on `company_id` in many jobs, each paying a shuffle.
Explain how bucketing removes that shuffle, like you'd explain a Redshift `DISTKEY`, and what it
costs."

## Rubric — a strong explanation hits these (5–7):

- [ ] Explains the small-file cost: per-file metadata/open, listing dominates, poor row-group alignment.
- [ ] Derives the output file count from **data size / target size**, not a hard-coded number.
- [ ] Distinguishes **in-memory partitions** (`repartition`/`coalesce`) from **on-disk partitions**
      (`partitionBy`), and says they meet at write time.
- [ ] Uses `coalesce` to reduce files cheaply (no shuffle) and `repartition` to rebalance/grow (shuffle).
- [ ] Warns against `coalesce(1)` on big data and `partitionBy` on a high-cardinality column.
- [ ] Explains bucketing: pre-shuffle on the key at write time so a later join on that key needs no
      shuffle (DISTKEY analogy), paid once at write, requires a managed table.
- [ ] (Bonus) Notes the file count ≈ in-memory partitions × on-disk partition folders.
