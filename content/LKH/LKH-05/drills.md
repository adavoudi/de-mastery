# LKH-05 · Interview drills

**1. "Your Iceberg table has 40k small files from streaming. What exactly do you run, and does it
delete the small files?"**
> `rewrite_data_files` (compaction) reads the small files and writes fewer right-sized ones,
> committing a new snapshot. It does **not** delete the old files — they remain referenced by prior
> snapshots so readers and time travel are unaffected. You reclaim them later with
> `expire_snapshots`. So the pair is compact (fix layout) then expire (reclaim disk).

**2. "You set `expire_snapshots(retain_last => 5)` and disk didn't shrink. Why, and what's the
fix?"**
> `expire_snapshots` also honors `max_snapshot_age_ms` (default ~5 days), which protects recent
> snapshots regardless of `retain_last`. To reclaim recent history you pass `older_than => TIMESTAMP
> '<cutoff>'`. But set that cutoff no earlier than your time-travel/rollback SLA — expiring your
> recovery window to save a little S3 is a classic self-inflicted outage.

**3. "SORTKEY on Redshift — what's the Iceberg equivalent and what does it actually buy you?"**
> `ALTER TABLE ... WRITE ORDERED BY col`. Compaction then writes files clustered by that column, so
> each file has tight min/max bounds. A predicate on the column lets the planner skip files whose
> bounds can't match (file pruning). It's statistics-driven skipping, same payoff as a SORTKEY:
> less data scanned, lower cost and latency.

**4. "A colleague wants to partition a huge events table by order_id. Talk them out of it."**
> One partition — and at least one small file — per distinct order_id. That's billions of tiny
> files and a gigantic manifest/metadata footprint; planning alone would be brutal. Partition on a
> low-cardinality transform like `days(event_ts)` or `bucket(32, order_id)`, and put the
> high-cardinality column in the **sort order** so you still get file pruning without the file
> explosion.

**5. "`expire_snapshots` ran but S3 still has stray files from a job that crashed. Why, and how do
you clean them?"**
> Those are **orphan** files — no snapshot ever referenced them, so `expire_snapshots` (which only
> follows snapshot references) can't see them. `remove_orphan_files` lists the table directory and
> deletes unreferenced files older than `older_than`. Keep that cutoff conservative (the ~3-day
> default) so you don't race and delete a file an in-flight commit is about to register.
