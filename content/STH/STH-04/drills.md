# STH-04 · Interview drills

**1. "When does a Flink Iceberg sink commit a snapshot, and why does that matter for file count?"**
> It commits exactly on each successful checkpoint. So the checkpoint interval is the freshness SLA
> *and* the file-production rate: each commit writes at least one data file per writer task per
> partition, so a 1-second interval produces ~60 snapshots and ~60 files a minute per partition.
> Freshness and small-files are the same dial — you buy latency with file count.

**2. "A streaming job writing to Iceberg hangs with zero commits. What's the likely cause?"**
> The source is unbounded (a `datagen`/`VALUES` source) and no checkpoint ever completes to drive
> the committer, or checkpointing is off. The fix for a backfill/test is a *bounded* source
> (`filesystem`/`debezium-json`): the job finishes, fires a final checkpoint, and commits. In
> production you enable periodic checkpointing so commits happen on the interval.

**3. "How do you keep sub-minute freshness without wrecking read performance on Iceberg?"**
> Accept that frequent commits make small files, and run compaction (`rewrite_data_files`) and
> snapshot expiration on a schedule to merge them back into large files. Freshness and file size are
> decoupled only by paying for compaction. If you need true sub-second freshness, put a stream store
> (Fluss/Kafka) in front and tier to Iceberg less often.

**4. "Same CDC feed into Iceberg v2 vs Paimon — how does each apply an update to an existing key?"**
> Iceberg v2 writes a *delete file* (equality or positional) plus the new row and merges them on
> read (merge-on-read); good when updates are rare. Paimon is an LSM primary-key table: the update
> lands as a new entry in a sorted run and background compaction merges runs by key — built for
> high-rate upserts, so reads stay cheap without a separate rewrite job.

**5. "Which would you choose for a CDC mirror of a high-churn OLTP table, and why?"**
> Paimon. The feed is mostly updates to the same primary keys; Iceberg v2's delete files and
> read-side merge grow with churn, while Paimon's LSM compaction is designed exactly for that
> pattern and keeps read amplification bounded. For a mostly-append event log, Iceberg is the
> simpler fit.

**6. "You committed 10,000 tiny snapshots overnight. What two maintenance jobs fix it, and what do
they each do?"**
> `rewrite_data_files` (compaction) merges the many small data files into fewer large ones, fixing
> read planning; `expire_snapshots` drops old snapshots and their now-unreferenced files, reclaiming
> storage and shrinking metadata. Compaction alone doesn't free space until old snapshots expire.
