# Stage 4 — Concurrent writers: conflict detection + retry (isolation)

Stage 3 gave you an atomic commit. Now put it under fire: **many writers at
once**. The invariant is **no lost updates** — if N writers each append their
rows, the final table must contain *every* row, exactly as if the appends had run
one after another (a serializable result equal to a serial oracle).

## Implement

Your commit loop from stage 3 already has the shape; make it robust:

1. Read the latest version and base metadata.
2. Build `N+1`.
3. `put_if_absent`. If it **fails**, another writer won the race → **re-read the
   now-latest metadata** and rebuild `N+1` from it, then try again. Bound the
   retries so a pathological case raises instead of spinning forever.

The critical detail the tests hunt for: on a conflict you must **re-read the
latest snapshot** before retrying. Retrying with the *stale* base silently drops
the winner's rows — a lost update.

## What the tests do

- Launch several concurrent writers (threads), released together by a
  `threading.Barrier` so they genuinely contend. **No `sleep()`.**
- Assert the final row multiset equals the serial oracle (every append applied).
- Assert the snapshot chain is well-formed (one commit per successful append; ids
  strictly increasing, no duplicates).

Keep stages 1–3 green.
