# Stage 6 — Manifest column stats + file pruning (planning speed)

A query that touches 2 of 10,000 files should **open only those 2**. The manifest
already lists each data file; now record **per-column min/max stats** at write
time, and use them at scan time to **skip** files that cannot match a filter. This
is the whole reason planning is fast on a lakehouse.

## Implement

- When you write a data file (`append`, and the compacted file in stage 7), record
  per-column `{min, max}` in that file's manifest entry.
- `table.scan(filter=(col, lo, hi))` takes an **inclusive** range filter. Before
  opening a data file, check its stats: if the file's `[min, max]` for `col`
  cannot overlap `[lo, hi]`, **skip the file entirely** (don't open it). Then still
  apply the row-level filter to the files you do open, so the returned rows are
  exactly those matching the predicate.
- Track every data file you physically open in `self.files_opened` (a list of
  keys). The tests read it to prove pruning happened.

## The classic bug to avoid

Bounds are **inclusive on both ends**. A file with `max == lo` or `min == hi`
*does* overlap and must be read. Using strict `<`/`>` here drops boundary rows —
the tests check exactly that.

Keep stages 1–5 green (an unfiltered `scan()` still returns everything).
