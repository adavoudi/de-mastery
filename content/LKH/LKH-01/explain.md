# LKH-01 · Explain it back (Feynman)

**Prompt A.** "I run `SELECT * FROM orders WHERE country = 'US'` in Athena. It says it scanned far
less than the whole table. Explain, level by level, how Iceberg decided which Parquet files to
read. Start at the Glue catalog pointer. End at the data files."

**Prompt B.** "A colleague says 'the table has 5 manifests, so it has 5 data files.' Explain why
this is wrong. Show which metadata you would read to get the real data-file count."

## Rubric — a strong explanation hits these (5–7):

- [ ] Starts at the single atomic pointer of the catalog to the current `metadata.json`.
- [ ] Names all four levels in order: metadata.json → manifest list → manifests → data files.
- [ ] Says the **manifest list** carries the partition ranges of each manifest (coarse pruning).
- [ ] Says the **manifests** carry per-file stats (min/max, null counts, record counts) for file
      pruning.
- [ ] Explains that pruning is **sound** (it never drops a file that may match). Also explains that
      it works like FND-06 row-group pruning, one level higher.
- [ ] Distinguishes "number of manifests" from "number of data files" (one manifest lists many
      files).
- [ ] (Bonus) Connects each write to a new snapshot, to immutability, and to atomic commit and
      time travel.
