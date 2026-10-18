# SPK-07 · Explain it back (Feynman)

**Prompt A.** "I have a pipeline that writes 5,000 tiny Parquet files. The downstream Athena-style
query is slow. Explain why this is slow. Explain how you would reach a target file *count/size*.
Be precise about `repartition` vs `coalesce` vs `partitionBy`."

**Prompt B.** "Two large tables are joined on `company_id` in many jobs. Each job pays for a shuffle.
Explain how bucketing removes that shuffle. Explain it like you would explain a Redshift `DISTKEY`.
Also explain what bucketing costs."

## Rubric — a strong explanation hits these (5–7):

- [ ] Explains the small-file cost: per-file metadata/open, listing takes most of the time, poor row-group alignment.
- [ ] Calculates the output file count from **data size / target size**, not a hard-coded number.
- [ ] Distinguishes **in-memory partitions** (`repartition`/`coalesce`) from **on-disk partitions**
      (`partitionBy`), and says they meet at write time.
- [ ] Uses `coalesce` to reduce files cheaply (no shuffle) and `repartition` to rebalance/grow (shuffle).
- [ ] Warns against `coalesce(1)` on big data and against `partitionBy` on a high-cardinality column.
- [ ] Explains bucketing: it pre-shuffles on the key at write time, so a later join on that key needs no
      shuffle (DISTKEY analogy). You pay once at write time. It requires a managed table.
- [ ] (Bonus) Notes the file count ≈ in-memory partitions × on-disk partition folders.
