# LKH-03 · Interview drills

**1. "The same Iceberg data is registered in Glue and in a REST catalog. What is actually different,
and what is the same?"**
> The data files and metadata files are identical and portable. The catalog does not own them.
> Three things differ: name resolution, the commit mechanism, and governance. The commit mechanism
> is a Glue/DynamoDB conditional update in one case. In the other case, it is the conditional
> commit of the REST protocol. When you switch catalogs, you change the control plane. You do not
> change the bytes.

**2. "Explain write-audit-publish and why you'd use a branch for it."**
> Write the new data to a short-lived branch. Then the production (`main`) readers are untouched.
> Audit the snapshot of the branch with quality checks. Publish only when the audit passes. To
> publish, fast-forward `main` to the branch tip (or cherry-pick the snapshot). A branch gives you
> an isolated staging area that you can inspect. It never exposes unaudited rows to `main`. It
> also gives you a cheap way to abort: drop the branch.

**3. "What's the difference between fast-forward and cherry-pick when publishing?"**
> Fast-forward moves `main` to the branch tip. It is valid when `main` has not diverged from the
> base of the branch. The history of the branch simply becomes the history of main. Cherry-pick
> applies a single snapshot of the branch onto the current `main`, even if main has moved on. It
> creates a new snapshot on main that reproduces the change of the branch. Use fast-forward for a
> clean linear publish. Use cherry-pick when main advanced concurrently.

**4. "A junior engineer's WAP job runs the row-count and null checks against `main` and then
fast-forwards. What's wrong?"**
> The candidate data lives on the branch, not on `main`. So the checks validate the *old*
> published state. They always pass, whatever the quality of the new data. The audit must read the
> snapshot of the branch (for example `VERSION AS OF 'audit'` or `t.branch_audit`). If you audit
> the wrong snapshot, the gate does nothing.

**5. "How does this map to a SOX control, and what would an auditor want to see?"**
> The branch is the staging area. The audit is the control test. The fast-forward is the approved
> publish. An auditor wants evidence of two facts. The gate ran against the candidate data. Nothing
> reached `main` without passing. The evidence includes these items: the snapshot id of the
> branch, the checks and their results, and the snapshot of the publish (fast-forward). All items
> are traceable in the snapshot history and metadata.
