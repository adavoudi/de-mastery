# LKH-05 · Interview drills

**1. "Your Iceberg table has 40k small files from streaming. What exactly do you run, and does it
delete the small files?"**
> `rewrite_data_files` (compaction) reads the small files and writes fewer files of the right size.
> It commits a new snapshot. It does **not** delete the old files. Prior snapshots still reference
> them, so readers and time travel are unaffected. You reclaim them later with `expire_snapshots`.
> So the pair is: compact (fix the layout), then expire (reclaim disk).

**2. "You set `expire_snapshots(retain_last => 5)` and disk didn't shrink. Why, and what's the
fix?"**
> `expire_snapshots` also uses `max_snapshot_age_ms` (default ~5 days). It protects recent
> snapshots, whatever the value of `retain_last`. To reclaim recent history, you pass
> `older_than => TIMESTAMP '<cutoff>'`. But set that cutoff no earlier than your time-travel and
> rollback SLA. If you expire your recovery window to save a little S3 space, you cause an outage
> yourself. This is a classic mistake.

**3. "SORTKEY on Redshift — what's the Iceberg equivalent and what does it actually buy you?"**
> `ALTER TABLE ... WRITE ORDERED BY col`. Then compaction writes files that are clustered by that
> column. So each file has tight min/max bounds. A predicate on the column lets the planner skip
> files whose bounds cannot match (file pruning). This skipping uses statistics. The payoff is the
> same as a SORTKEY: less data scanned, and lower cost and latency.

**4. "A colleague wants to partition a huge events table by order_id. Talk them out of it."**
> This creates one partition, and at least one small file, for each distinct order_id. That is
> billions of tiny files and a huge metadata and manifest footprint. Planning alone would be very
> slow. Partition on a low-cardinality transform such as `days(event_ts)` or
> `bucket(32, order_id)`. Put the high-cardinality column in the **sort order**. Then you still
> get file pruning, and you avoid the explosion of files.

**5. "`expire_snapshots` ran but S3 still has stray files from a job that crashed. Why, and how do
you clean them?"**
> Those are **orphan** files. No snapshot ever referenced them. `expire_snapshots` only follows
> snapshot references, so it cannot see them. `remove_orphan_files` lists the table directory and
> deletes unreferenced files that are older than `older_than`. Keep that cutoff conservative (the
> ~3-day default). Then you do not race with an in-flight commit and delete a file that it is
> about to register.
