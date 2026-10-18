# LKH-02 · Commits & concurrency

## Why commits are the hard part

In LKH-01 you saw that a commit is "just" an atomic swap of the catalog's single pointer to a new
`metadata.json`. That sounds trivial until two writers try to commit at the same time — two
overlapping dbt runs, a backfill racing the hourly job, a streaming sink and a compaction job. If
both read the table at version *N*, each builds a new metadata on top of *N*, and both blindly
install their result, **one write is silently lost**: whoever swaps the pointer second erases the
first writer's snapshot from the current line. Lost updates are the quiet data-corruption bug that
table formats exist to prevent.

Iceberg's answer is **optimistic concurrency control (OCC)**. There are no locks held across the
work. Each writer optimistically does its work against the snapshot it read, then, at commit time,
performs an **atomic compare-and-swap**: "install my new metadata *only if* the table is still at
the version I started from." If the table moved underneath it (someone else committed first), the
swap fails and the writer is told so with a conflict error. Optimism pays off when conflicts are
rare, which they usually are.

## How a commit actually works

1. **Read** the current `metadata.json` (version *N*, the base snapshot).
2. **Do the work** against that base: write data files, build the new manifests and manifest list,
   assemble metadata version *N+1* whose parent is *N*.
3. **Commit = compare-and-swap** at the catalog: atomically move the pointer from *N* to *N+1*
   **only if** it is still *N*. The mechanism depends on the catalog (LKH-03): a Glue/DynamoDB
   conditional update, a REST catalog's conditional commit, or an object-store *put-if-absent* on
   the next metadata filename.
4. **On conflict** (the pointer was no longer *N*): the catalog rejects the swap and the client
   raises a conflict (`CommitFailedException` in PyIceberg). Your *N+1* now sits on a stale base.

The fix is **refresh and re-apply**: reload the table to pick up the winner's new state, recompute
your change against *that* fresh base, and try the compare-and-swap again — with bounded retries.
Because the operation is recomputed on fresh data each time, no update is lost: the final state is
*as if the two writers had run one after another* (serial equivalence).

Two subtleties:

- **Not every overlap is a true conflict.** Two pure appends to different files commute: Iceberg
  can often re-apply an append onto the new base automatically without you recomputing anything.
  The conflicts that *must* be re-applied by hand are **read-modify-write** operations (overwrite a
  row by a value you read, delete-then-insert) — because the value you based your change on may
  have changed.
- **Isolation levels.** Iceberg supports *serializable* and *snapshot* isolation. Under
  serializable isolation a conflicting concurrent change to rows your operation depended on is
  rejected; under snapshot isolation some commuting changes are allowed through. The default for
  overwrites/merges is `serializable`.

## From your world

| dbt / warehouse | Iceberg OCC | Note |
|---|---|---|
| Two dbt runs writing one table | two writers racing to swap the pointer | last-writer-wins = lost update if naive |
| A warehouse transaction `COMMIT` | catalog compare-and-swap of metadata | atomic, all-or-nothing |
| "retry the model on deadlock" | refresh-and-reapply with bounded retries | recompute on the fresh base, don't blind-retry |
| `SERIALIZABLE` vs `READ COMMITTED` | serializable vs snapshot isolation | same vocabulary |

## When NOT to rely on OCC the naive way

- **Don't blind-retry.** Re-submitting the *same* precomputed metadata after a conflict either
  conflicts again or, worse, installs a value computed from a stale read → the lost update you were
  trying to avoid. Always `refresh()` and recompute first.
- **Don't catch the conflict and swallow it.** `except CommitFailedException: pass` turns a loud,
  recoverable conflict into a silent lost write. Catch it only to refresh-and-retry.
- **Don't use OCC for high-contention, tiny, hot updates.** If hundreds of writers hammer the same
  rows every second, optimism degrades into a retry storm; that is a KV-store / OLTP workload, not
  a lakehouse table (the track's "when not to use it").

## Further reading

- Iceberg spec — Optimistic Concurrency: <https://iceberg.apache.org/spec/#optimistic-concurrency>
- Iceberg spec — Commit Conflict Resolution and Retry:
  <https://iceberg.apache.org/spec/#commit-conflict-resolution-and-retry>
- Iceberg reliability / concurrent writes:
  <https://iceberg.apache.org/docs/latest/reliability/#concurrent-write-operations>
- PyIceberg transactions & table API (refresh, overwrite):
  <https://py.iceberg.apache.org/api/>
