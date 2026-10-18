# STH-03 · Interview drills

**1. "What is Apache Fluss and what gap does it fill between Kafka and a lakehouse?"**
> Fluss is a columnar, low-latency streaming storage layer. Kafka is a cheap low-latency log but a
> poor place for keyed latest-value lookups or columnar OLAP; a lakehouse is cheap OLAP but only at
> batch latency. Fluss stores recent data hot (sub-second) and tiers history to the lake
> (Paimon/Iceberg) as one logical table, serving both latencies from one copy.

**2. "Log table vs PK table in Fluss?"**
> A log table is append-only — a columnar Kafka topic you read as a stream. A PK table
> (`PRIMARY KEY(...) NOT ENFORCED`) is upsertable: it serves the latest value per key with low
> latency and lets you read either the changelog (streaming) or a point-in-time snapshot (batch).

**3. "What does lake tiering do, and how do you read the two tiers?"**
> With `'table.datalake.enabled'='true'`, a Flink tiering service continuously moves committed data
> from Fluss's hot store into the lake format (Paimon). Reading the table name gives the real-time
> view (hot + tiered); reading `` `table$lake` `` gives just the tiered lake view, readable in batch
> by Flink or by pure-Python pypaimon with no Flink at all.

**4. "State the read-consistency invariant for a tiered table and why it matters."**
> After tiering has quiesced, a streaming read of the table and a batch read of the lake tier must
> return the same set of rows. If they disagree, the streamhouse's promise — one copy, two
> latencies, the same answer — is broken, which is exactly the reconciliation failure analytics
> engineers chase between a fast store and a warehouse.

**5. "Why must a batch read of the `$lake` tier be bounded with a deadline?"**
> Tiering is asynchronous and the underlying source can be unbounded, so an unbounded read would
> never return. You read with a wall-clock deadline (and a bounded scan) so the job finishes once
> the tiered data has landed — typically ~30–40 s — instead of hanging on a live stream.

**6. "When would you NOT reach for Fluss + tiering?"**
> When you only need batch OLAP (write straight to Paimon/Iceberg), when you only need a log (plain
> Kafka/Fluss log table), or when the data is small and low-QPS enough that one database serves both
> the lookup and the scan. A tiered streaming store earns its operational cost only at scale or
> under strict freshness SLAs.
