# FLK-09 · Interview drills

**1. "How do you deduplicate a stream in Flink SQL, and is the result append-only?"**
> `ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts) = 1`. With an ascending ORDER BY you keep the
> first row per key and the query is append-only (later duplicates are dropped, nothing retracts).
> Flipping to `ORDER BY ts DESC` keeps the last row and becomes an updating query.

**2. "A Top-N query feeds a Kafka topic that downstream consumers read. What can go wrong with a
plain kafka sink?"**
> Top-N is updating: when a new value enters the top-N it retracts the displaced row. A plain append
> `kafka` sink can't express the retraction, so consumers see stale rows and double counts. Use
> `upsert-kafka` keyed by the entity so updates/deletes are tombstoned correctly.

**3. "UNION vs UNION ALL in streaming — which do you reach for and why?"**
> `UNION ALL`. It concatenates and stays append-only. `UNION` (distinct) must hold unbounded state
> to suppress duplicates across the whole stream — expensive and an updating query. If I truly need
> dedup, I bound it with a time window or an explicit keep-first dedup.

**4. "When would you use MATCH_RECOGNIZE instead of a windowed aggregation?"**
> When the rule is a *sequence* — A then B then C in time order within a key — rather than a count
> or sum over a window. Fraud ("login, 3 failures, password change in 5 min"), funnels, SLA
> breaches. MATCH_RECOGNIZE maintains per-partition NFA state and projects via MEASURES; ORDER BY
> must be a time attribute.

**5. "Explain the difference between OVER and GROUP BY to a SQL analyst."**
> GROUP BY collapses rows into one per key with an updating aggregate. OVER keeps every row and
> attaches a running aggregate computed over preceding rows (ORDER BY a time attribute) — like a SQL
> window function. OVER is append-only: one output row per input row, no retractions.

**6. "You need the latest address per customer as a continuously-updated table. Which pattern, and
which sink?"**
> Dedup keep-last (`ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ts DESC) = 1`). It's an
> updating query, so the sink must be upsert-capable — `upsert-kafka` keyed by `customer_id`, a JDBC
> table with that primary key, or a lakehouse upsert.
