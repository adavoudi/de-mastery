# STH-04 · Interview drills

**1. "When does a Flink Iceberg sink commit a snapshot, and why does that matter for file count?"**
> It commits exactly on each successful checkpoint. So the checkpoint interval is the freshness SLA
> *and* the rate at which the sink produces files. Each commit writes at least one data file per
> writer task per partition. A 1-second interval produces ~60 snapshots and ~60 files a minute per
> partition. Freshness and small files are the same setting. You buy latency with file count.

**2. "A streaming job writing to Iceberg hangs with zero commits. What's the likely cause?"**
> The source is unbounded (a `datagen`/`VALUES` source), and no checkpoint ever completes to drive
> the committer. Or checkpointing is off. For a backfill or test, the fix is a *bounded* source
> (`filesystem`/`debezium-json`). Then the job finishes, fires a final checkpoint and commits. In
> production, you enable periodic checkpointing. Then commits happen on the interval.

**3. "How do you keep sub-minute freshness without wrecking read performance on Iceberg?"**
> Accept that frequent commits make small files. Run compaction (`rewrite_data_files`) and snapshot
> expiration on a schedule. They merge the small files back into large files. You separate
> freshness from file size only if you pay for compaction. Suppose you need true sub-second
> freshness. Then put a stream store (Fluss/Kafka) in front and tier to Iceberg less often.

**4. "Same CDC feed into Iceberg v2 vs Paimon — how does each apply an update to an existing key?"**
> Iceberg v2 writes a *delete file* (equality or positional) plus the new row. It merges them on
> read (merge-on-read). This is good when updates are rare. Paimon is an LSM primary-key table. The
> update lands as a new entry in a sorted run. Background compaction merges the runs by key. Paimon
> is built for high-rate upserts, so reads stay cheap without a separate rewrite job.

**5. "Which would you choose for a CDC mirror of a high-churn OLTP table, and why?"**
> Paimon. The feed is mostly updates to the same primary keys. The delete files and the read-side
> merge of Iceberg v2 grow with churn. Paimon's LSM compaction is designed exactly for that pattern.
> It keeps read amplification bounded. For a mostly-append event log, Iceberg is the simpler fit.

**6. "You committed 10,000 tiny snapshots overnight. What two maintenance jobs fix it, and what do
they each do?"**
> - `rewrite_data_files` (compaction) merges the many small data files into fewer large ones. This
>   fixes read planning.
> - `expire_snapshots` drops old snapshots and their files that nothing references now. This
>   reclaims storage and shrinks metadata.
>
> Compaction alone does not free space until old snapshots expire.
