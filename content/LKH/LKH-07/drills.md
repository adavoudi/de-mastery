# LKH-07 · Interview drills

**1. "When would you actually choose Hudi over Iceberg or Delta?"**
> When the workload is mutable and upsert-heavy — CDC ingestion, frequent primary-key updates — and
> you want "give me only what changed since T" incremental pulls. Hudi's record-level index maps keys
> to file groups so an upsert touches only the affected files, and its timeline drives incremental
> queries. For append-only analytics that machinery is just overhead.

**2. "Iceberg vs Delta for a greenfield batch lakehouse — how do you decide?"**
> For batch they're feature-comparable, so decide on ecosystem fit. Databricks + Unity Catalog +
> liquid clustering → Delta (native). Vendor-neutral, many query engines, want free partition
> evolution and hidden partitioning → Iceberg. I would not decide on a benchmark run on a workload
> unlike ours.

**3. "You run Iceberg but a new tool only reads Delta. Migrate?"**
> Usually no. Use interop: Delta UniForm (expose Iceberg-compatible metadata) or Apache XTable
> (translate metadata between formats) so one copy of the data is readable as both. Migration is
> costly and interop solves the real need — another engine reading the data — without moving it.

**4. "Explain Copy-on-Write vs Merge-on-Read and when each wins."**
> CoW rewrites whole data files on update: more expensive writes, cheap reads, simplest. MoR appends
> delete/log files and merges at read time: cheap, fast writes but reads do extra merge work until
> compaction. Upsert-heavy / streaming favors MoR; read-heavy / infrequent-update favors CoW. All
> three formats offer both.

**5. "What's the single most overrated factor in the format debate, and the most underrated?"**
> Overrated: micro-benchmarks — they rarely match your workload and the formats converge. Underrated:
> the catalog and governance layer (Unity, Polaris, Glue, Nessie) and your existing engine ecosystem,
> which drive day-to-day operability far more than the on-disk format.
