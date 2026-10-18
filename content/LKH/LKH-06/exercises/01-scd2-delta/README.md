# SCD2 upsert in Delta Lake (MERGE)

You did Slowly Changing Dimension Type 2 with Iceberg `MERGE` in LKH-04. Now do the *same* task in
**Delta Lake**, so you can read its transaction log and compare. This is a dbt-snapshot-style
dimension that keeps full history.

The customer dimension `delta_dim` has columns `(bkey INT, attr STRING, valid_from STRING, valid_to
STRING)`. A row is the **current** version when `valid_to IS NULL`. A daily batch of updates arrives
as a temp view with columns `(bkey, attr, eff)` where `eff` is the effective date of the change.

Implement `scd2_merge(spark, table, updates_view)` in `solution.py`. For every row in the updates
view it must, as a correct Type 2 upsert:

1. **Close** the current version of any key whose attribute **changed**: set `valid_to = eff` on the
   open row (`valid_to IS NULL`) when the incoming `attr` differs.
2. **Insert** a new current version (`valid_to = eff` as `valid_from`, `valid_to = NULL`) for every
   key that is **new** or whose attribute **changed**.
3. Leave keys whose attribute is **unchanged** exactly as they are (still one open row, no new row).

It must **never overwrite the `attr` of an existing row** — that would be SCD Type 1 and destroy
history.

The table is created with `delta.enableChangeDataFeed=true`, so your two-step MERGE will show up in
the Change Data Feed.

The test checks, with an independent DuckDB oracle:

- the set of **current** rows (`valid_to IS NULL`) equals the oracle's expected current dimension;
- **exactly one open row per key** (no key left with two current versions, none closed by mistake);
- a **closed history row** exists for every changed key (history is preserved, not overwritten);
- the Delta **history / `_delta_log`** grew (commits were made);
- the **Change Data Feed** emits `update_preimage`/`update_postimage` for the closed rows and
  `insert` for the new versions.

### Hint on structure

Two `MERGE` statements is the clean way: one to close changed rows, one to insert new versions. The
second MERGE's `ON` clause should include `t.attr = u.attr` so unchanged keys remain matched (and
are not re-inserted) while changed/new keys fall through to `WHEN NOT MATCHED THEN INSERT`.
