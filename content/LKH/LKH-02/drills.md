# LKH-02 · Interview drills

**1. "What exactly makes an Iceberg commit atomic, and where does the atomicity live?"**
> The commit is an atomic swap of the catalog's single pointer from the base `metadata.json` to the
> new one, implemented as a compare-and-swap (Glue/DynamoDB conditional update, a REST catalog's
> conditional commit, or an object-store put-if-absent on the next metadata filename). All the data
> and manifest writing happens before that; only the pointer swap is the commit, and it either
> fully succeeds or fails.

**2. "Describe a lost update on an Iceberg table and how OCC prevents it."**
> Two writers read version N. Each computes a change from N and tries to install N+1. Without
> conflict detection, the second swap overwrites the first and that update is gone. OCC makes the
> swap conditional on the table still being at N; the second writer's swap fails, so it must
> refresh to the first writer's result and re-apply, which preserves both changes.

**3. "A teammate's retry loop does `while True: try: commit() except CommitFailedException: commit()`.
What's wrong?"**
> It re-submits metadata computed from a stale base without refreshing, so it either conflicts
> again forever or, if the operation is an overwrite, installs a value derived from the old read —
> a lost update. The retry must first `refresh()` the table and recompute the change on the new
> current snapshot, and it should bound the number of retries.

**4. "Which operations commute, and why does that matter for conflict handling?"**
> Pure appends to different files commute: order doesn't change the result, so Iceberg can often
> re-apply an append onto the new base automatically. Read-modify-write operations (overwrite a row
> by a read value, delete-then-insert) do not commute, because the value you based the change on may
> have changed; those are the ones your retry logic must recompute.

**5. "How would you test that your concurrent writer is correct?"**
> Define the serial oracle: run the two operations one after another and record the final state.
> Then run them concurrently with the refresh-and-reapply loop and assert the final state equals the
> serial oracle (no lost update, no double-apply). Mutants — blind retry, no refresh, swallow the
> exception — must each fail that assertion.
