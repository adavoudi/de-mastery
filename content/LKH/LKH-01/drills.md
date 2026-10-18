# LKH-01 · Interview drills

**1. "Walk me through what happens, metadata-wise, when I append one batch to an Iceberg table."**
> Iceberg writes a new data file (or files) to object storage. It writes a new manifest that lists
> those files with their per-file stats. It writes a new manifest list. This list references the
> new manifest and the manifests of the previous snapshot that are still live. It writes a new
> `metadata.json`. This file adds a new snapshot (which points at that manifest list) and marks it
> as current. Last, the single pointer of the catalog is atomically swapped to the new
> `metadata.json`. Iceberg changed no existing file.

**2. "How does Iceberg prune files for a filtered query, and how is that different from Parquet
row-group pruning?"**
> There are two levels. First, the manifest list lets the planner skip each manifest whose
> partition-value range cannot match. Then the per-file min/max/null stats of each remaining
> manifest let it skip single data files. The idea is the same min/max idea as Parquet row-group
> pruning (FND-06). But it works one level higher, over files, before Iceberg opens any data file.
> Parquet pruning then happens *again* inside the files that the engine actually reads.

**3. "What's the difference between the manifest list and a manifest?"**
> The manifest list is the top index of a snapshot. It has one row per manifest. Each row holds the
> partition ranges of that manifest and the counts of added, existing and deleted files. A manifest
> is the next level down. It has one row per *file*. Each row holds the path, the record count and
> the column min/max/null stats of that file. The manifest list is an index of manifests. A
> manifest is an index of files.

**4. "Why can't I just equate the number of manifests to the number of data files?"**
> A manifest references many files. A single manifest can list thousands of data files. This is
> common after compaction rewrites many small files into a few manifests. The two counts are at
> different levels of the tree. In general, they are not related.

**5. "I expired old snapshots but storage didn't drop. Which part of the tree explains why a given
data file is still referenced?"**
> Walk the manifest list and the manifests of every live snapshot. Collect the data files that they
> reference. A file is an orphan only when the manifests of no *live* snapshot reference it. An
> older retained snapshot may still reference the file. In that case, expiring a newer snapshot does
> not free it. (Snapshot expiry and orphan-file cleanup are in LKH-05. But the reasoning is only
> reachability in the metadata tree.)
