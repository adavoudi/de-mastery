# STH-06 · Interview drills

**1. "Why is a quarantine/DLQ better than filtering bad records with a WHERE clause?"**
> A `WHERE good` silently discards records, so the mart looks clean while quietly missing rows no one
> can account for. A quarantine routes each failing record to a side table with the reason it failed,
> so every input is either valued in the mart or explained in the DLQ — auditable, triageable and
> replayable. In a stream there is no post-load gate, so the choice is route-or-lose.

**2. "What single invariant proves a DQ stage didn't silently drop anything?"**
> Completeness: `count(mart) + count(quarantine) == count(input)`, with the two sets disjoint. If a
> record leaves the input and lands in neither, the counts stop reconciling. It is the streaming form
> of "the load balanced".

**3. "You have three rules (not-null customer, positive amount, valid customer FK). How do you split
the stream?"**
> Evaluate the same predicate two ways: the good stream requires ALL rules to pass; the quarantine
> stream requires ANY rule to fail. The two predicates must be exact logical complements so no record
> goes to both and none to neither. The good stream feeds the as-of fact join; the bad stream gets a
> reason tag.

**4. "A record violates two rules at once. What reason do you record, and how many quarantine rows?"**
> Exactly one quarantine row. The reason is the first matching rule in a `CASE` (a chosen priority
> order); membership in the quarantine does not depend on that order, only the label does.

**5. "Map these DQ rules to the dbt tests you already write."**
> not-null customer ↔ `not_null`; positive amount ↔ accepted-values / a range test; valid customer
> reference ↔ `relationships` (FK). The difference is timing: dbt runs them after the load and fails
> the run; in-flight DQ runs them per event and quarantines instead of blocking.

**6. "How do you keep a DLQ from becoming a silent drop with extra steps?"**
> Treat it as a queue, not a graveyard: monitor its size and reasons, alert on spikes, and give
> operators a path to fix the source and replay the quarantined records back through the pipeline. An
> unwatched quarantine loses data just as surely as a `WHERE` filter — the routing only helps if
> someone reads it.
