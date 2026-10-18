# LKH-01 · Interview drills

**1. "Walk me through what happens, metadata-wise, when I append one batch to an Iceberg table."**
> A new data file (or files) is written to object storage. A new manifest is written listing those
> files with their per-file stats. A new manifest list is written referencing the new manifest plus
> the still-live manifests from the previous snapshot. A new `metadata.json` is written adding a new
> snapshot (pointing at that manifest list) and marking it current. Finally the catalog's single
> pointer is atomically swapped to the new `metadata.json`. Nothing existing was mutated.

**2. "How does Iceberg prune files for a filtered query, and how is that different from Parquet
row-group pruning?"**
> Two levels. First the manifest list lets the planner skip whole manifests whose partition-value
> range can't match. Then each surviving manifest's per-file min/max/null stats let it skip
> individual data files. It's the same min/max idea as Parquet row-group pruning (FND-06) but one
> level up — over files, before any data file is opened. Parquet pruning then happens *again* inside
> the files that are actually read.

**3. "What's the difference between the manifest list and a manifest?"**
> The manifest list is the snapshot's top index: one row per manifest, with that manifest's
> partition ranges and added/existing/deleted counts. A manifest is the next level down: one row per
> *file*, with that file's path, record count and column min/max/null stats. List = index of
> manifests; manifest = index of files.

**4. "Why can't I just equate the number of manifests to the number of data files?"**
> Because a manifest references many files. A single manifest can list thousands of data files,
> especially after compaction rewrites many small files into few manifests. The counts live at
> different levels of the tree and are unrelated in general.

**5. "I expired old snapshots but storage didn't drop. Which part of the tree explains why a given
data file is still referenced?"**
> Walk every live snapshot's manifest list → manifests and collect referenced data files; a file is
> only orphaned when no *live* snapshot's manifests reference it. If an older retained snapshot still
> references it, expiring a newer one won't free it. (Snapshot expiry and orphan-file cleanup are
> LKH-05, but the reasoning is pure metadata-tree reachability.)
