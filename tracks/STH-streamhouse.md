# STH: Streamhouse (~35 h)

**Philosophy.** Lambda architecture runs two pipelines, batch and streaming, which often disagree.
Kappa runs one streaming pipeline, but Kafka storage is expensive to query historically. The
lakehouse made object storage transactional, but only at batch latency. A **streamhouse** puts
a stream-native storage layer on the lake, so one copy of the data serves streaming reads
(changelogs) and batch/OLAP reads, usually with Flink as the engine. The term was coined by
Ververica, and is now shared across several streaming vendors (Aiven, Confluent, Redpanda,
StreamNative, Ververica), who have since formed a Streamhouse working group.
**When not to use it:** if hourly freshness is fine, a batch lakehouse is simpler and cheaper.

Prerequisites: LKH, KFK, FLK. Lab profile: `streamhouse`.
