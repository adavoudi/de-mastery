# STH-06 · Interview drills

**1. "Why is a quarantine/DLQ better than filtering bad records with a WHERE clause?"**
> A `WHERE good` silently discards records. The mart looks clean, but it quietly misses rows that no
> one can account for. A quarantine routes each failing record to a side table with the reason it
> failed. So the mart values every input or the DLQ explains it. You can audit, triage and replay
> the records. A stream has no post-load gate. So the choice is to route the record or to lose it.

**2. "What single invariant proves a DQ stage didn't silently drop anything?"**
> Completeness: `count(mart) + count(quarantine) == count(input)`, with the two sets disjoint.
> Suppose a record leaves the input and lands in neither table. Then the counts stop reconciling.
> This is the streaming form of "the load balanced".

**3. "You have three rules (not-null customer, positive amount, valid customer FK). How do you split
the stream?"**
> Evaluate the same predicate in two ways. The good stream requires that ALL rules pass. The
> quarantine stream requires that ANY rule fails. The two predicates must be exact logical
> complements. Then no record goes to both and none goes to neither. The good stream feeds the
> as-of fact join. The bad stream gets a reason tag.

**4. "A record violates two rules at once. What reason do you record, and how many quarantine rows?"**
> Exactly one quarantine row. The reason is the first matching rule in a `CASE` (a priority order
> that you choose). Membership in the quarantine does not depend on this order. Only the label does.

**5. "Map these DQ rules to the dbt tests you already write."**
> - Not-null customer ↔ `not_null`.
> - Positive amount ↔ accepted-values / a range test.
> - Valid customer reference ↔ `relationships` (FK).
>
> The difference is timing. dbt runs the tests after the load and fails the run. In-flight DQ runs
> them for each event and quarantines the record. It does not block the pipeline.

**6. "How do you keep a DLQ from becoming a silent drop with extra steps?"**
> Treat it as a queue and not as a place where data stays forever. Monitor its size and its reasons.
> Alert on spikes. Give operators a way to fix the source and replay the quarantined records back
> through the pipeline. An unwatched quarantine loses data just as surely as a `WHERE` filter. The
> routing helps only if someone reads it.
