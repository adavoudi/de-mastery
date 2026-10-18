# FLK-04 · Interview drills

**1. "What does it mean that a Flink SQL query returns a dynamic table?"**
> The query runs continuously, and its result keeps changing as input arrives. That result is a
> dynamic table. The engine emits its changes as a changelog stream. These are rows with the tags
> +I/-U/+U/-D. A dbt model gives you a result. A Flink SQL query gives you a changelog, and the
> folded state of this changelog is that result.

**2. "Trace the changelog of an unbounded GROUP BY COUNT as x, y, x arrive."**
> First, +I (x,1). Then +I (y,1). When the second x arrives, the engine retracts the old row with
> -U (x,1) and emits the new row +U (x,2). The final state is {x:2, y:1}. But the changelog contains
> a retraction. The query is an updating query, not an append-only query.

**3. "Append-only vs updating — how do I tell, and why does it matter?"**
> Append-only queries only emit +I and never revise past output (filter/project, window TVF
> aggregations). Updating queries emit -U/+U/-D (unbounded GROUP BY, latest-per-key, non-windowed
> joins). It matters because an updating query needs an upsert sink (upsert-kafka) keyed by the
> grain. If you append its revisions to a plain topic, you duplicate every change.

**4. "Why does the course grade a ported model on its changelog, not just the final total?"**
> A query can print the right number once and still be the wrong kind of query. A check of the
> RowKinds proves that the query updates correctly. A check that the folded final state equals the
> DuckDB batch answer proves that the query converges to the right result. Together, the two checks
> catch SQL that is too weak and SQL that is subtly wrong.

**5. "Which sink for an updating COUNT per customer, and what does a null value mean?"**
> upsert-kafka keyed by customer. It keeps the latest value per key (a compacted changelog). A record
> with a null value is a tombstone. It deletes that key. This is the dynamic table materialised as a
> Kafka-backed, keyed table.

**6. "A tumbling-window hourly sum vs an unbounded running sum — same SQL shape, different changelog.
Explain."**
> The window TVF aggregation emits each hour once, when the window closes. It is therefore
> append-only (+I per window). The unbounded running sum revises its single row per key forever. It
> is therefore updating (-U/+U). The GROUP BY instinct is the same. The window boundary makes one
> query append and the other query update.
