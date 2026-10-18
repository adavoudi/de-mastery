# Stage 2 — Snapshots & time travel (immutability)

Every `append` already wrote a new metadata version. Now expose that history as
**snapshots**, and let a reader travel back to any past snapshot. This works only
because data files are **immutable**: an old snapshot still points at files that
were never overwritten.

## Implement

- A `Snapshot` object (or namedtuple-like) exposing at least `snapshot_id`,
  `version`, `timestamp_ms`, and `manifest`.
- `table.snapshots() -> list[Snapshot]` — every committed snapshot, oldest first,
  including the empty create snapshot.
- `table.current_snapshot() -> Snapshot` — the latest.
- `table.scan(snapshot_id=<id>)` — read the table **as of** that snapshot, i.e.
  only the data files that snapshot's manifest listed. With no `snapshot_id`, scan
  the current snapshot (stage-1 behavior).

## Why it matters

Time travel and auditing (SOX!) fall straight out of immutable data + a chain of
metadata versions. A reader that captured `snapshot_id` keeps seeing exactly those
rows no matter how many appends happen afterward.

Keep stage-1 tests green: `scan()` with no arguments is still "latest".
