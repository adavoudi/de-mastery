# LKH-03 · Interview drills

**1. "The same Iceberg data is registered in Glue and in a REST catalog. What is actually different,
and what is the same?"**
> The data and metadata files are identical and portable — the catalog doesn't own them. What
> differs is name resolution, the commit mechanism (Glue/DynamoDB conditional update vs the REST
> protocol's conditional commit), and governance. Switching catalogs changes the control plane, not
> the bytes.

**2. "Explain write-audit-publish and why you'd use a branch for it."**
> Write the new data to a short-lived branch so production (`main`) readers are untouched. Audit the
> branch's snapshot with quality checks. Publish only on pass by fast-forwarding `main` to the branch
> tip (or cherry-picking the snapshot). A branch gives you an isolated, inspectable staging area that
> never exposes unaudited rows to `main`, with a cheap "drop the branch" abort path.

**3. "What's the difference between fast-forward and cherry-pick when publishing?"**
> Fast-forward moves `main` to the branch tip and is valid when `main` hasn't diverged from the
> branch's base — the branch's history simply becomes main's. Cherry-pick applies a single branch
> snapshot onto the current `main` even if main has moved on, creating a new snapshot on main that
> reproduces the branch change. Use fast-forward for a clean linear publish, cherry-pick when main
> advanced concurrently.

**4. "A junior engineer's WAP job runs the row-count and null checks against `main` and then
fast-forwards. What's wrong?"**
> The candidate data lives on the branch, not on `main`, so the checks validate the *old* published
> state and always pass regardless of the new data's quality. The audit must read the branch's
> snapshot (e.g. `VERSION AS OF 'audit'` or `t.branch_audit`). Auditing the wrong snapshot is a
> no-op gate.

**5. "How does this map to a SOX control, and what would an auditor want to see?"**
> The branch is the staging area, the audit is the control test, and the fast-forward is the
> approved publish. An auditor wants evidence that the gate ran against the candidate data and that
> nothing reached `main` without passing — the branch snapshot id, the checks and their results, and
> the publish (fast-forward) snapshot, all traceable in snapshot history/metadata.
