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

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| STH-01 | Architecture history | 3 | Lambda → Kappa → lakehouse → streamhouse; the trade-offs of each; vendor landscape | Classify 5 real architectures (Yelp, Uber, Netflix, ...), graded with a rubric | Your current batch lakehouse |
| STH-02 | Apache Paimon | 6 | LSM tree on the lake, primary-key tables, merge engines (dedup, partial-update, aggregation), changelog producers | Build a partial-update customer table from 3 streams | MDM golden record |
| STH-03 | Apache Fluss | 5 | Columnar streaming storage, log tables and PK tables, tiering to the lake, delta joins | Fluss PK table + lake tiering; read it in streaming and batch mode with identical results | – |
| STH-04 | Iceberg as streaming storage | 4 | Flink Iceberg sink/source, incremental reads, commit frequency vs. small files, topic → table (e.g. Tableflow) | Tune commit interval vs. file count; compare with Paimon | Your Iceberg tables |
| STH-05 | Streaming dimensional modeling | 6 | SCD2 from CDC, versioned tables, temporal joins, conformed dimensions, facts with retractions | Streaming star schema: `fct_orders` + SCD2 `dim_customer`, reconciled to a batch rebuild | **Your core strength** |
| STH-06 | Streaming quality & contracts | 4 | In-flight DQ rules, quarantine/DLQ, completeness checks with watermarks, reconciliation | Add DQ + quarantine to STH-05; mutants with silent drops must fail | dbt tests, SOX controls |
| STH-07 | Serving layer | 4 | StarRocks/Trino on Paimon and Iceberg, Flink materialized tables, OLAP vs. streaming results | Dashboard query under 1 s on the streaming mart | Athena/Redshift serving |
| STH-08 | Latency tiers & cost | 3 | Seconds / minutes / hours: what each tier costs in compute, storage, operations and people | Cost model for 3 freshness tiers on the same mart, checked within ±20% | Daily SLA trade-offs |
