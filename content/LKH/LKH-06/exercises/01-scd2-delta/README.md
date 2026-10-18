# SCD2 upsert in Delta Lake (MERGE)

You did Slowly Changing Dimension Type 2 with Iceberg `MERGE` in LKH-04. Now do the *same* task in
**Delta Lake**. Then you can read its transaction log and compare. This is a dimension in the style
of a dbt snapshot. It keeps full history.

The customer dimension `delta_dim` has the columns `(bkey INT, attr STRING, valid_from STRING,
valid_to STRING)`. A row is the **current** version when `valid_to IS NULL`. A daily batch of
updates arrives as a temp view with the columns `(bkey, attr, eff)`. Here, `eff` is the effective
date of the change.

Implement `scd2_merge(spark, table, updates_view)` in `solution.py`. For every row in the updates
view, it must do these steps, as a correct Type 2 upsert:

1. **Close** the current version of any key whose attribute **changed**. Set `valid_to = eff` on
   the open row (`valid_to IS NULL`) when the incoming `attr` is different.
2. **Insert** a new current version (`valid_from = eff`, `valid_to = NULL`) for every
   key that is **new** or whose attribute **changed**.
3. Leave keys whose attribute is **unchanged** exactly as they are (still one open row, no new
   row).

It must **never overwrite the `attr` of an existing row**. That would be SCD Type 1 and it would
destroy history.

The table is created with `delta.enableChangeDataFeed=true`. So your two-step MERGE shows up in the
Change Data Feed.

The test uses an independent DuckDB oracle. It checks these facts:

- The set of **current** rows (`valid_to IS NULL`) equals the current dimension that the oracle
  expects.
- There is **exactly one open row per key**. No key has two current versions. No key was closed by
  mistake.
- A **closed history row** exists for every changed key (history is preserved, not overwritten).
- The Delta **history / `_delta_log`** grew (commits were made).
- The **Change Data Feed** emits `update_preimage` and `update_postimage` for the closed rows. It
  emits `insert` for the new versions.

### Hint on structure

The clean way is two `MERGE` statements. One closes the changed rows. The other inserts the new
versions. The `ON` clause of the second MERGE should include `t.attr = u.attr`. Then unchanged keys
stay matched (and Delta does not insert them again). Changed and new keys fall through
to `WHEN NOT MATCHED THEN INSERT`.
