# LKH-01 · Explain it back (Feynman)

**Prompt A.** "I run `SELECT * FROM orders WHERE country = 'US'` in Athena and it says it scanned
far less than the whole table. Trace, level by level, how Iceberg decided which Parquet files to
read — starting from the Glue catalog pointer and ending at the data files."

**Prompt B.** "A colleague says 'the table has 5 manifests so it has 5 data files.' Explain why
that is wrong, and show which metadata you'd read to get the real data-file count."

## Rubric — a strong explanation hits these (5–7):

- [ ] Starts at the catalog's single atomic pointer to the current `metadata.json`.
- [ ] Names all four levels in order: metadata.json → manifest list → manifests → data files.
- [ ] Says the **manifest list** carries per-manifest partition ranges (coarse pruning).
- [ ] Says the **manifests** carry per-file stats (min/max, null/record counts) for file pruning.
- [ ] Explains pruning is **sound** (never drops a possibly-matching file) and maps to FND-06
      row-group pruning one level up.
- [ ] Distinguishes "number of manifests" from "number of data files" (one manifest lists many).
- [ ] (Bonus) Connects each write → new snapshot → immutability → atomic commit + time travel.
