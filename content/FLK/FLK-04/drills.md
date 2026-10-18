# FLK-04 · Interview drills

**1. "What does it mean that a Flink SQL query returns a dynamic table?"**
> The query runs continuously and its result keeps changing as input arrives. That result is a
> dynamic table, and the engine emits its changes as a changelog stream — rows tagged +I/-U/+U/-D.
> A dbt model gives you a result; a Flink SQL query gives you a changelog whose folded state is that
> result.

**2. "Trace the changelog of an unbounded GROUP BY COUNT as x, y, x arrive."**
> +I (x,1); +I (y,1); then for the second x the engine retracts the old row -U (x,1) and emits the new
> one +U (x,2). The final state is {x:2, y:1}, but the changelog contains a retraction — it is an
> updating query, not append-only.

**3. "Append-only vs updating — how do I tell, and why does it matter?"**
> Append-only queries only ever emit +I and never revise past output (filter/project, window TVF
> aggregations). Updating queries emit -U/+U/-D (unbounded GROUP BY, latest-per-key, non-windowed
> joins). It matters because an updating query needs an upsert sink (upsert-kafka) keyed by the grain;
> appending its revisions to a plain topic would duplicate every change.

**4. "Why does the course grade a ported model on its changelog, not just the final total?"**
> Because a query can print the right number once yet be the wrong kind of query. Checking the
> RowKinds proves it updates correctly, and checking that the folded final state equals the DuckDB
> batch answer proves it converges to the right result — catching both too-weak and subtly-wrong SQL.

**5. "Which sink for an updating COUNT per customer, and what does a null value mean?"**
> upsert-kafka keyed by customer. It keeps the latest value per key (a compacted changelog); a record
> with a null value is a tombstone — a delete for that key. That's exactly the dynamic table
> materialised as a Kafka-backed, keyed table.

**6. "A tumbling-window hourly sum vs an unbounded running sum — same SQL shape, different changelog.
Explain."**
> The window TVF aggregation emits each hour once when the window closes, so it is append-only (+I
> per window). The unbounded running sum revises its single row per key forever, so it is updating
> (-U/+U). Same GROUP BY instinct, but the window boundary is what makes one append and the other
> update.
