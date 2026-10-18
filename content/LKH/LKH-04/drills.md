# LKH-04 · Interview drills

**1. "Explain copy-on-write vs merge-on-read and when you'd pick each."**
> CoW rewrites each affected data file on an update/delete, so reads always hit clean files (fast
> reads) but writes rewrite whole files even for one row (write amplification). MoR instead writes a
> small delete file (position, equality, or a v3 deletion vector) and leaves data files alone, so
> writes are cheap but reads merge deletes at scan time until compaction. Pick CoW for read-heavy,
> rarely-written tables; pick MoR for high-frequency upserts/CDC.

**2. "How do you prove, from metadata alone, that a delete used MoR and not CoW?"**
> Query `SELECT content, count(*) FROM t.files GROUP BY content`. MoR adds a position-delete file,
> so you see `content=1` alongside `content=0`. CoW adds no delete file and instead rewrites the
> data file, so you see only `content=0` but the data-file set changed (old file replaced). It's a
> measurable property, not a timing measurement.

**3. "Describe the two-step SCD2 MERGE and why it's two steps."**
> Step 1 is a `MERGE` that closes current rows whose attribute changed (`WHEN MATCHED AND
> t.attr <> u.attr THEN UPDATE SET valid_to=<batch>, is_current=false`). Step 2 inserts a new open
> version for both changed keys and brand-new keys (an anti-join against the current rows). It's two
> steps because a single `MERGE` can't both close an old version and insert a new row for the same
> key — the update and the insert target different rows.

**4. "A teammate's SCD2 job does `UPDATE dim SET attr = new_attr WHERE nk = ...`. What's wrong?"**
> That overwrites the current row in place, destroying the historical value — it turns a Type 2
> dimension into Type 1. The correct action closes the old version (sets `valid_to`,
> `is_current=false`) and inserts a new current row, preserving the full history.

**5. "What are the off-by-one and 'open valid_to' SCD2 bugs, and how would a test catch them?"**
> Off-by-one sets the new version's `valid_from` or the old version's `valid_to` one day off, so a
> key's intervals overlap or leave a gap — detectable by asserting the old `valid_to` equals the new
> `valid_from` (contiguous, non-overlapping). 'Open valid_to' forgets to close the superseded row, so
> a key has two rows with `is_current=true` / `valid_to IS NULL` — detectable by asserting exactly
> one current row per key. A DuckDB oracle computing the correct close/insert catches both by direct
> row comparison.
