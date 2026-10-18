# LKH-02 · Interview drills

**1. "What exactly makes an Iceberg commit atomic, and where does the atomicity live?"**
> The commit is an atomic swap of the single pointer of the catalog. The pointer moves from the
> base `metadata.json` to the new one. The swap is a compare-and-swap. It can be a Glue/DynamoDB
> conditional update, a conditional commit of a REST catalog, or an object-store put-if-absent on
> the next metadata filename. All the data writes and manifest writes happen before the swap. Only
> the pointer swap is the commit. It either fully succeeds or it fails.

**2. "Describe a lost update on an Iceberg table and how OCC prevents it."**
> Two writers read version N. Each writer computes a change from N and tries to install N+1.
> Imagine that there is no conflict detection. Then the second swap overwrites the first, and that
> update is gone. OCC makes the swap conditional on the table still being at N. The swap of the
> second writer fails. The second writer must refresh to the result of the first writer and
> re-apply its change. This preserves both changes.

**3. "A teammate's retry loop does `while True: try: commit() except CommitFailedException: commit()`.
What's wrong?"**
> The loop re-submits metadata that was computed from a stale base. It does not refresh first. So
> it conflicts again, forever. Or, if the operation is an overwrite, it installs a value that came
> from the old read. That is a lost update. The retry must first `refresh()` the table. Then it
> must recompute the change on the new current snapshot. It should also bound the number of
> retries.

**4. "Which operations commute, and why does that matter for conflict handling?"**
> Pure appends to different files commute. The order does not change the result. So Iceberg can
> often re-apply an append onto the new base automatically. Read-modify-write operations do not
> commute. Examples are an overwrite of a row by a value that you read, and delete-then-insert.
> The value that your change used may have changed. Your retry logic must recompute these
> operations.

**5. "How would you test that your concurrent writer is correct?"**
> First, define the serial oracle. Run the two operations one after another and record the final
> state. Then run them concurrently with the refresh-and-reapply loop. Assert that the final state
> equals the serial oracle (no lost update, no double-apply). Mutants must each fail that
> assertion. The mutants are blind retry, no refresh, and swallow the exception.
