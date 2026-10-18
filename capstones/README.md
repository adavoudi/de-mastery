# Projects & capstones (~120 h)

**Is one streamhouse capstone enough? No.** A streamhouse capstone exercises Flink, Kafka,
Paimon/Fluss and Iceberg. It does **not** exercise:

- large Spark batch work: backfills, skew, shuffle tuning, PB-scale layout (the most common interview topic);
- the Kafka developer depth of CCDAK (producer/consumer semantics, Kafka Streams, transactions);
- orchestration, idempotent reruns and cost at batch scale.

Those three gaps are the questions other companies ask most often. So there are **three capstones**.
Each builds on the previous one and reuses the same synthetic multi-company commerce domain, and
together they form one platform story for your CV.

| | C1: Batch lakehouse | C2: Streaming CDC ledger | C3: Streamhouse mart |
|---|---|---|---|
| After | SPK + LKH | KFK (+ JVM) | FLK + STH |
| Hours | 35 | 35 | 50 |
| One-line pitch | Multi-company commerce mart on Spark + Iceberg, sized for PB | SOX-grade exactly-once ledger from OLTP CDC to the lake | Real-time star schema with SCD2, one storage layer for streaming and batch |

## C1: Multi-company batch lakehouse (Spark + Iceberg)

- **Sources:** generated OLTP exports for 3 companies (one acquired mid-year, with different
  product and customer keys), FX rates, ~100 GB locally.
- **Build:** Spark pipelines into bronze/silver/gold Iceberg tables; a conformed `dim_customer`
  (MDM match/merge) and `dim_product`; SCD2 with `MERGE INTO`; `fct_orders` and `fct_payments`; WAP
  publish via Iceberg branches; Dagster/Airflow orchestration with idempotent backfills.
- **Prove:** a scaling experiment at 1/10/100 GB (runtime, shuffle and file counts) plus an
  extrapolation to 1 PB with a cost model. `💲💲 optional`: one 1 TB run on EMR Serverless to check the model.
- **Tests:** DuckDB oracle on gold tables; mutants for SCD2 and FX conversion; maintenance checks
  (file size, snapshot expiry).

## C2: Exactly-once streaming CDC ledger (Kafka + Kafka Streams/Flink + Iceberg)

- **Sources:** Postgres order/payment/refund service using the outbox pattern.
- **Build:** Debezium → Kafka (Avro + Schema Registry, compatibility rules, data contracts) →
  Kafka Streams enrichment (EOS v2) → double-entry ledger topic → Iceberg via Flink or Connect.
  The topics are multi-tenant by company. A DLQ and quarantine path for bad events.
- **Prove:** a fault-injection suite (kill brokers, Connect workers and app instances; inject
  network faults with Toxiproxy). Hourly reconciliation shows `source == Kafka == lake` and
  `sum(debit) == sum(credit)`. A SOX control matrix (ARC-07) with automated evidence.
- **Ops:** secured cluster (TLS + SASL + ACLs), Grafana dashboards for lag and URPs, and a runbook.
  This gives CCAAK practice.

## C3: Streamhouse real-time commerce mart (Flink + Paimon/Fluss + Iceberg)

- **Build:** Flink SQL + DataStream jobs read C2's topics and build a **streaming Kimball star
  schema**: SCD2 `dim_customer` from CDC, a partial-update golden record (Paimon merge engine)
  that unifies customers across companies, and `fct_orders` enriched by temporal joins at FX rate
  as of order time. Served through StarRocks/Trino. Batch backfills use **Spark on the same tables**,
  which reuses C1.
- **Prove:**
  - streaming result == batch rebuild (the G6 oracle at capstone scale);
  - a stateful upgrade with a savepoint and no data loss;
  - a latency-tier cost comparison (seconds/minutes/hours);
  - a design doc for running it at 1 PB/year, including DR (ARC-09 format).
- **Optional `💲`:** run one Flink job on AWS Managed Service for Apache Flink or Confluent Cloud
  (trial) to compare operations.

## Coverage matrix

| Skill | Track projects | C1 | C2 | C3 |
|---|---|---|---|---|
| Spark batch tuning, skew, AQE | SPK | ●●● | | ● |
| Spark Structured Streaming | SPK-10/11 | | | ● |
| Iceberg internals & maintenance | LKH | ●●● | ● | ●● |
| Kafka producer/consumer/EOS | KFK | | ●●● | ● |
| Schema Registry & contracts | KFK-06 | | ●●● | ● |
| Kafka Connect / Debezium CDC | KFK-07 | | ●●● | ● |
| Kafka Streams | KFK-08 | | ●● | |
| Kafka ops & security | KFK-11..14 | | ●● | |
| Flink SQL / DataStream / state | FLK | | ● | ●●● |
| Paimon / Fluss | STH | | | ●●● |
| Streaming Kimball / SCD2 | STH-05 | ●(batch) | ● | ●●● |
| MDM / multi-company | ARC-08 | ●●● | ● | ●●● |
| SOX / audit / reconciliation | ARC-07 | ● | ●●● | ●● |
| Orchestration & backfills | ARC-03 | ●●● | | ● |
| Cloud warehouses (Snowflake/BigQuery/Databricks/Fabric) | CDW | ● (gold layer ported in CDW-17) | | |
| Cloud warehouses (Snowflake/BigQuery/Databricks/Fabric) | CDW | ● (gold layer ported in CDW-17) | | |
| PB-scale design & cost | ARC-02, ARC-09 | ●●● | ● | ●●● |

Each capstone is shipped as a public GitHub repo containing a README, an architecture diagram,
a design doc, test results and a short demo video. These, not the certificates, are what go on your CV.
