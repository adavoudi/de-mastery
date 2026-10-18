# Stage 3 — Atomic commit via put-if-absent (optimistic concurrency)

So far one writer bumps the version. What makes two writers safe? A commit must be
**atomic**: either `v<N+1>.json` becomes visible in full, or not at all, and two
writers must never both believe they created the same version.

Object stores give you exactly one tool for this: a **conditional create** —
"write this key only if it does not already exist". `objectstore.py` exposes it as
`put_if_absent(key, data) -> bool` (True = you created it). On S3 this is the
`If-None-Match: *` conditional PUT; here it's `O_CREAT|O_EXCL`. **You build the
commit protocol on it; you never re-implement the primitive.**

## Implement (refactor your commit path)

Make every mutation (`append`, and later `delete`/`compact`) go through one commit
routine:

1. Read the latest version `N` and its metadata (the *base*).
2. Build the new snapshot `N+1` from the base.
3. `ok = store.put_if_absent("<name>/metadata/v{N+1}.json", data)`.
4. If `ok` → committed, return the new snapshot id.
   If **not** `ok` → someone else committed `N+1` first. Go back to step 1 and
   retry against the *new* latest (this is the stage-4 retry loop; wire it in now).

## What the tests check

- A commit is all-or-nothing: no half-written metadata is ever observable.
- You use `put_if_absent` for the version object (a plain overwrite would let two
  writers clobber each other — that's a lost update, tested in stage 4).

Keep stages 1–2 green.
