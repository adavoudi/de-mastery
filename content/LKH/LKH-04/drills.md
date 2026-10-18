# LKH-04 · Interview drills

**1. "Explain copy-on-write vs merge-on-read and when you'd pick each."**
> CoW rewrites each affected data file on an update or delete. So reads always hit clean files
> (fast reads). But writes rewrite whole files, even for one row (write amplification). MoR writes
> a small delete file (position, equality, or a v3 deletion vector) and leaves the data files
> alone. So writes are cheap. But reads merge the deletes at scan time until compaction. Pick CoW
> for tables that you read often and write rarely. Pick MoR for high-frequency upserts and CDC.

**2. "How do you prove, from metadata alone, that a delete used MoR and not CoW?"**
> Query `SELECT content, count(*) FROM t.files GROUP BY content`. MoR adds a position-delete file.
> So you see `content=1` next to `content=0`. CoW adds no delete file. It rewrites the data file.
> So you see only `content=0`, but the set of data files changed (a new file replaced the old
> file). This is a measurable property. It is not a timing measurement.

**3. "Describe the two-step SCD2 MERGE and why it's two steps."**
> Step 1 is a `MERGE` that closes current rows whose attribute changed (`WHEN MATCHED AND
> t.attr <> u.attr THEN UPDATE SET valid_to=<batch>, is_current=false`). Step 2 inserts a new open
> version for changed keys and for brand-new keys (an anti-join against the current rows). There
> are two steps because a single `MERGE` cannot close an old version and insert a new row for the
> same key. The update and the insert target different rows.

**4. "A teammate's SCD2 job does `UPDATE dim SET attr = new_attr WHERE nk = ...`. What's wrong?"**
> This overwrites the current row in place. It destroys the historical value. It turns a Type 2
> dimension into a Type 1 dimension. The correct action closes the old version (it sets
> `valid_to` and `is_current=false`). Then it inserts a new current row. This preserves the full
> history.

**5. "What are the off-by-one and 'open valid_to' SCD2 bugs, and how would a test catch them?"**
> The off-by-one bug sets the `valid_from` of the new version, or the `valid_to` of the old
> version, one day off. Then the intervals of a key overlap or leave a gap. A test catches this
> when it asserts that the old `valid_to` equals the new `valid_from` (contiguous, no overlap).
> The 'open valid_to' bug forgets to close the superseded row. Then a key has two rows with
> `is_current=true` and `valid_to IS NULL`. A test catches this when it asserts exactly one
> current row per key. A DuckDB oracle computes the correct close and insert. It catches both bugs
> by direct row comparison.
