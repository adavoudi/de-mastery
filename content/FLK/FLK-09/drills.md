# FLK-09 · Interview drills

**1. "How do you deduplicate a stream in Flink SQL, and is the result append-only?"**
> Use `ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts) = 1`. With an ascending ORDER BY, you keep the
> first row per key and the query is append-only. The query drops later duplicates and nothing retracts.
> If you change to `ORDER BY ts DESC`, the query keeps the last row and becomes an updating query.

**2. "A Top-N query feeds a Kafka topic that downstream consumers read. What can go wrong with a
plain kafka sink?"**
> Top-N is updating. When a new value enters the top-N, the query retracts the displaced row. A plain append
> `kafka` sink cannot express the retraction. So consumers see stale rows and double counts. Use
> `upsert-kafka` keyed by the entity. Then Flink writes tombstones correctly for updates and deletes.

**3. "UNION vs UNION ALL in streaming — which do you use and why?"**
> `UNION ALL`. It concatenates the streams and stays append-only. `UNION` (distinct) must hold unbounded state
> to suppress duplicates across the whole stream. It is expensive and it is an updating query. If I really need
> dedup, I bound it with a time window or I use an explicit keep-first dedup.

**4. "When would you use MATCH_RECOGNIZE instead of a windowed aggregation?"**
> Use it when the rule is a *sequence*: A then B then C in time order within a key. A windowed aggregation
> gives a count or a sum over a window instead. Examples of sequences are fraud ("login, 3 failures, password change in 5 min"), funnels and SLA
> breaches. MATCH_RECOGNIZE maintains NFA state for each partition and projects with MEASURES. ORDER BY
> must be a time attribute.

**5. "Explain the difference between OVER and GROUP BY to a SQL analyst."**
> GROUP BY collapses rows into one row per key with an updating aggregate. OVER keeps every row and
> attaches a running aggregate. The aggregate uses the preceding rows (ORDER BY a time attribute). It is like a SQL
> window function. OVER is append-only: one output row per input row, no retractions.

**6. "You need the latest address per customer as a continuously-updated table. Which pattern, and
which sink?"**
> Use dedup keep-last (`ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ts DESC) = 1`). It is an
> updating query, so the sink must support upserts. Use `upsert-kafka` keyed by `customer_id`, a JDBC
> table with that primary key, or a lakehouse upsert.
