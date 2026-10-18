# Stage 7 — Compaction + snapshot expiration (maintenance)

Many small appends and delete files make reads slow. Maintenance fixes that
**without** breaking readers pinned to old snapshots.

## Implement

- `table.compact() -> snapshot_id` — read all **live** rows of the current
  snapshot (data minus deletes), write them into **one new data file**, and commit
  a snapshot whose manifest is that single file with **no delete files**. Do
  **not** physically remove the old data/delete files yet — a reader time-traveling
  to an earlier snapshot still needs them.
- `table.expire(older_than_ms) -> int` — drop snapshots whose `timestamp_ms` is
  `< older_than_ms` (always keep the current one), then **garbage-collect** any
  data or delete file that **no retained snapshot** references. Return the number
  of files physically deleted.

## The invariants the tests enforce

- After `compact()`, the current scan is unchanged (same rows) but the current
  manifest is a single file with no deletes.
- A reader pinned to a **pre-compaction** snapshot still reads the correct old
  rows — because compaction only *adds*; it never deletes the old files.
- `expire` must **never** delete a file still referenced by a snapshot it kept.
  Expiring old snapshots first makes their now-unreferenced files collectible; the
  live snapshot's files are always retained.

Keep stages 1–6 green.
