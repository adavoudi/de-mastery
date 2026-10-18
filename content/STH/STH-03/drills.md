# STH-03 · Interview drills

**1. "What is Apache Fluss and what gap does it fill between Kafka and a lakehouse?"**
> Fluss is a columnar, low-latency streaming storage layer. Kafka is a cheap, low-latency log. But
> it is a poor place for keyed latest-value lookups or columnar OLAP. A lakehouse is cheap OLAP, but
> only at batch latency. Fluss stores recent data hot (sub-second) and tiers history to the lake
> (Paimon/Iceberg) as one logical table. It serves both latencies from one copy.

**2. "Log table vs PK table in Fluss?"**
> A log table is append-only. It is a columnar Kafka topic that you read as a stream. A PK table
> (`PRIMARY KEY(...) NOT ENFORCED`) is upsertable. It serves the latest value per key with low
> latency. You can read either the changelog (streaming) or a point-in-time snapshot (batch).

**3. "What does lake tiering do, and how do you read the two tiers?"**
> You set `'table.datalake.enabled'='true'`. Then a Flink tiering service continuously moves
> committed data from the hot store of Fluss into the lake format (Paimon). If you read the table
> name, you get the real-time view (hot + tiered). If you read `` `table$lake` ``, you get only the
> tiered lake view. You can read it in batch with Flink, or with pure-Python pypaimon and no Flink
> at all.

**4. "State the read-consistency invariant for a tiered table and why it matters."**
> Tiering has quiesced. Then a streaming read of the table and a batch read of the lake tier must
> return the same set of rows. If they disagree, the streamhouse breaks its promise: one copy, two
> latencies, the same answer. This is exactly the reconciliation failure that analytics engineers
> chase between a fast store and a warehouse.

**5. "Why must a batch read of the `$lake` tier be bounded with a deadline?"**
> Tiering is asynchronous, and the underlying source can be unbounded. So an unbounded read would
> never return. You read with a wall-clock deadline (and a bounded scan). Then the job finishes when
> the tiered data has landed, typically in ~30–40 s. It does not hang on a live stream.

**6. "When would you NOT reach for Fluss + tiering?"**
> There are three cases.
> - You only need batch OLAP. Write straight to Paimon/Iceberg.
> - You only need a log. Use plain Kafka or a Fluss log table.
> - The data is small and the QPS is low, so one database serves both the lookup and the scan.
>
> A tiered streaming store is worth its operational cost only at scale or under strict freshness
> SLAs.
