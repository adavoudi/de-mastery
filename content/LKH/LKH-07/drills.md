# LKH-07 · Interview drills

**1. "When would you actually choose Hudi over Iceberg or Delta?"**
> Choose Hudi when the workload is mutable and upsert-heavy (CDC ingestion, frequent primary-key
> updates). Also choose it when you want incremental pulls: "give me only what changed since T".
> The record-level index of Hudi maps keys to file groups. So an upsert touches only the affected
> files. Its timeline drives incremental queries. For append-only analytics, that machinery is
> only overhead.

**2. "Iceberg vs Delta for a greenfield batch lakehouse — how do you decide?"**
> For batch, they have comparable features. So decide on ecosystem fit. Databricks + Unity Catalog
> + liquid clustering → Delta (native). Vendor-neutral, many query engines, and you want free
> partition evolution and hidden partitioning → Iceberg. I would not decide on a benchmark that ran
> on a workload unlike ours.

**3. "You run Iceberg but a new tool only reads Delta. Migrate?"**
> Usually no. Use interop. Use Delta UniForm (it exposes Iceberg-compatible metadata) or Apache
> XTable (it translates metadata between formats). Then you can read one copy of the data as both
> formats. Migration is costly. Interop solves the real need, which is another engine that reads
> the data. It does this without moving the data.

**4. "Explain Copy-on-Write vs Merge-on-Read and when each wins."**
> CoW rewrites whole data files on update. Writes are more expensive, reads are cheap, and it is
> the simplest. MoR appends delete or log files and merges them at read time. Writes are cheap and
> fast. But reads do extra merge work until compaction. Upsert-heavy and streaming workloads favor
> MoR. Read-heavy workloads with infrequent updates favor CoW. All three formats offer both.

**5. "What's the single most overrated factor in the format debate, and the most underrated?"**
> Overrated: micro-benchmarks. They rarely match your workload, and the formats converge.
> Underrated: the catalog and governance layer (Unity, Polaris, Glue, Nessie) and your existing
> engine ecosystem. They drive day-to-day operability far more than the on-disk format.
