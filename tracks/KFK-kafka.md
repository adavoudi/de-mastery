# KFK: Apache Kafka (~65 h)

**Philosophy.** Kafka is a distributed, replicated, append-only **log**. Producers and consumers
are decoupled in time: data is retained and can be replayed. It is the central nervous system
between operational systems and analytics. **When not to use it:** request/response workflows,
simple task queues with few consumers (SQS/RabbitMQ is simpler), very small data volumes, or
analytics that only need a daily batch.

Kafka 4.x runs on KRaft only (no ZooKeeper). Lab profile: `streaming`. Clients: Python
(`confluent-kafka`) for KFK-01..07. Java for Kafka Streams (KFK-08) and for the CCDAK-style
exercises. Cert tags: `dse-found`, `ccdak:<domain>`, `ccaak:<domain>` (see
[certs/README.md](../certs/README.md)).

| ID | Module | h | Key ideas | Exercises (graded) | From your world | Certs |
|---|---|---|---|---|---|---|
| KFK-01 | The log | 3 | Why a log; events vs. state; retention; replay; Kafka vs. queues vs. a database | Replay a topic from an offset or timestamp and rebuild a table | Iceberg snapshots ≈ log positions | dse-found, ccdak:1, ccaak:1 |
| KFK-02 | Topics, partitions, replication | 5 | Leaders, ISR, `acks`, `min.insync.replicas`, unclean election, KRaft controllers | Kill a broker; predict which writes succeed under each `acks` setting | – | ccdak:1, ccaak:1 |
| KFK-03 | Producers | 5 | Batching, `linger.ms`, compression, partitioners, retries, idempotence, ordering | Tune for throughput; the test asserts ordering per key survives retries | – | dse-found, ccdak:2 |
| KFK-04 | Consumers & groups | 6 | Group protocol (incl. KIP-848), rebalancing, commit strategies, lag, share groups (KIP-932) | At-least-once consumer with manual commits; no loss across crashes | – | dse-found, ccdak:2 |
| KFK-05 | Transactions & exactly-once | 5 | Idempotent producer, transactional read-process-write, `read_committed`, zombie fencing | Exactly-once copier; fault-injection test checks ledger invariants | SOX completeness/accuracy | ccdak:2 |
| KFK-06 | Schema Registry & contracts | 5 | Avro/Protobuf/JSON Schema, subject naming, compatibility modes, evolution, data contracts and rules | Evolve the order schema without breaking consumers; the test checks compatibility | dbt contracts, conformed dimensions | dse-found, ccdak:2 |
| KFK-07 | Kafka Connect & CDC | 6 | Workers, connectors, tasks, converters, SMTs, DLQ; Debezium, outbox pattern, snapshots | Postgres → Debezium → Kafka; derive SCD2 events from the change stream | SCD2 sourced from OLTP | dse-found, ccdak:2, ccaak:4 |
| KFK-08 | Kafka Streams | 7 | KStream/KTable/GlobalKTable, joins, windows, state stores, changelogs, EOS v2 | Enrich orders with customers; tested with `TopologyTestDriver` | dbt joins, but continuous | dse-found, ccdak:3 |
| KFK-09 | Event & topic design | 4 | Key choice, partition count, compaction, event vs. entity topics, multi-tenancy, ordering | Design topics for multi-company commerce, graded with a rubric and a property test on keys | Fact grain; multi-company mart | ccdak:1 |
| KFK-10 | Testing & observability (apps) | 4 | Mocks vs. `TopologyTestDriver` vs. Testcontainers; client metrics, lag, tracing | Add metrics + lag alerting to your KFK-05 app | dbt test/CI | ccdak:4, ccdak:5 |
| KFK-11 | Cluster configuration | 5 | Broker and topic configs, sizing, tiered storage, quotas, reassignment | Rebalance partitions with minimal movement | Redshift WLM | ccaak:3, ccaak:5 |
| KFK-12 | Security | 4 | TLS, SASL (SCRAM/OAUTHBEARER), ACLs, RBAC concepts, encryption | Lock down the lab cluster; tests check that a disallowed principal is denied | SOX access controls | ccaak:2 |
| KFK-13 | Deployment architecture | 3 | Rack awareness, multi-region, MirrorMaker 2, Cluster Linking, MSK/Confluent/WarpStream/diskless | Design memo: 2-region DR with RPO/RTO numbers | – | ccaak:3 |
| KFK-14 | Operating & troubleshooting | 3 | Key JMX metrics, under-replicated partitions, consumer lag, rolling upgrades | 6 "broken cluster" scenarios; find and fix each one | Incident runbooks | ccaak:6, ccaak:7 |

## KFK-BYO: build your own mini Kafka (~10 h, on top of the module hours; included in the ROADMAP total)

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
