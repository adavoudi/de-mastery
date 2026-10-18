# LKH-06 · Interview drills

**1. "Delta `_delta_log` vs. Iceberg metadata — how is 'the current state' defined in each?"**
> Delta: replay (fold) the ordered JSON commits in `_delta_log/`. Each commit lists `add` and
> `remove` actions. Checkpoints every ~10 commits let readers skip ahead. Iceberg: the catalog
> points at a single current `metadata.json`. This file names a manifest list. The manifest list
> names manifests. The manifests name data files. Both formats are atomic and append-only. The
> difference is a log fold vs. a tree with one pointer.

**2. "Write the SCD2 MERGE and point out the bug that turns it into SCD1."**
> Match on the business key where `valid_to IS NULL`. When the row matches and the attribute
> changed, run `UPDATE SET valid_to = effective_date` (close it). Then a second step inserts the
> new current row with `valid_to = NULL`. The SCD1 bug is `UPDATE SET attr = u.attr`. This
> overwrites the attribute in place and destroys history. SCD2 never changes the old attribute.

**3. "A single UPDATE under CDF — what change records come out, and when can you NOT read them?"**
> `update_preimage` (the row before) and `update_postimage` (the row after). Each has a
> `_commit_version`. You cannot read changes that happened *before* someone set
> `delta.enableChangeDataFeed=true`. CDF is not retroactive, and it is not free. It adds write
> cost and change-data files.

**4. "When would you pick Delta over Iceberg, honestly?"**
> Pick Delta on Databricks, where Delta is the native format with the best support (Unity Catalog,
> liquid clustering, Lakeflow Spark Declarative Pipelines (SDP)). Or pick it when a required tool only speaks Delta. Suppose you have a
> vendor-neutral lake that already runs Iceberg on Trino/Athena. Then a switch gives you little.
> The formats have comparable features for batch work. Choose on ecosystem fit, not on folklore.

**5. "What are liquid clustering and UniForm, each in one sentence, and their Iceberg analogue?"**
> Liquid clustering is an incrementally maintained `CLUSTER BY`. It replaces static partitions +
> ZORDER for data skipping. Iceberg fills this role with sort order + hidden partitioning. UniForm
> writes Iceberg-compatible metadata next to the Delta log. Then Iceberg engines can read a Delta
> table without a copy of the data.
