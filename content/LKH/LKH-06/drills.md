# LKH-06 · Interview drills

**1. "Delta `_delta_log` vs. Iceberg metadata — how is 'the current state' defined in each?"**
> Delta: replay (fold) the ordered JSON commits in `_delta_log/`, each listing `add`/`remove`
> actions; checkpoints every ~10 commits let readers skip ahead. Iceberg: the catalog points at a
> single current `metadata.json`, which names a manifest list, which names manifests, which name
> data files. Both are atomic and append-only; the difference is log-fold vs. a one-pointer tree.

**2. "Write the SCD2 MERGE and point out the bug that turns it into SCD1."**
> Match on the business key where `valid_to IS NULL`; when matched and the attribute changed,
> `UPDATE SET valid_to = effective_date` (close it); then a second step inserts the new current row
> with `valid_to = NULL`. The SCD1 bug is `UPDATE SET attr = u.attr` — overwriting the attribute in
> place destroys history. SCD2 never mutates the old attribute.

**3. "A single UPDATE under CDF — what change records come out, and when can you NOT read them?"**
> `update_preimage` (row before) and `update_postimage` (row after), each with `_commit_version`.
> You cannot read changes that occurred *before* `delta.enableChangeDataFeed=true` was set — CDF is
> not retroactive and is not free; it adds write cost and change-data files.

**4. "When would you pick Delta over Iceberg, honestly?"**
> On Databricks, where Delta is the native, best-supported format (Unity Catalog, liquid clustering,
> SDP). Or when a required tool only speaks Delta. For a vendor-neutral lake already running Iceberg
> on Trino/Athena, switching buys little — the formats are feature-comparable for batch. Choose on
> ecosystem fit, not folklore.

**5. "What are liquid clustering and UniForm, each in one sentence, and their Iceberg analogue?"**
> Liquid clustering is an incrementally maintained `CLUSTER BY` that replaces static partitions +
> ZORDER for data skipping — the role Iceberg fills with sort order + hidden partitioning. UniForm
> writes Iceberg-compatible metadata beside the Delta log so Iceberg engines can read a Delta table
> without copying data.
