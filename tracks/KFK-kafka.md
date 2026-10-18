# KFK: Apache Kafka (~65 h)

**Philosophy.** Kafka is a distributed, replicated, append-only **log**. Producers and consumers
are decoupled in time: data is retained and can be replayed. It is the central nervous system
between operational systems and analytics. **When not to use it:** request/response workflows,
simple task queues with few consumers (SQS/RabbitMQ is simpler), very small data volumes, or
analytics that only need a daily batch.

Kafka 4.x runs on KRaft only (no ZooKeeper). Lab profile: `streaming`. Clients: Python
(`confluent-kafka`) for KFK-01..07. Java for Kafka Streams (KFK-08).

## KFK-BYO: build your own mini Kafka (~10 h, in addition to the modules)

The tests use real Kafka clients (`kcat`, `confluent-kafka`) speaking the actual Kafka wire protocol
to your broker, in the spirit of CodeCrafters "Build your own Kafka".

| Stage | Build | Teaches |
|---|---|---|
| 1 | TCP server, request header parsing, ApiVersions | The wire protocol |
| 2 | Segmented log on disk with an offset index | Storage |
| 3 | Produce (single partition) | Batches, CRC |
| 4 | Fetch by offset | Consumer reads |
| 5 | Metadata + multiple topics/partitions | Partitioning |
| 6 | Retention and log compaction | Retention semantics |
| 7 | Consumer group offsets (OffsetCommit/OffsetFetch) | Commits |
| 8 | (optional) Follower replication + high watermark | ISR |
