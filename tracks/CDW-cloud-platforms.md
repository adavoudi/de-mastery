# CDW: Cloud data platforms: Snowflake, BigQuery, Databricks, Fabric (~60 h)

**Philosophy.** Most job postings name a platform, not an engine: "Snowflake + dbt",
"BigQuery on GCP", "Databricks lakehouse", "Microsoft Fabric". Under the marketing they all
make the same few choices: storage separated from compute on object storage, columnar
micro-partitions with metadata pruning, a pricing unit (credits, slots, DBUs, capacity units),
role-based governance and, more and more, open table formats (Iceberg/Delta). This track teaches
those shared ideas **once** (CDW-01..03), then each platform as a set of choices on top of
them, so a new platform takes days to learn, not months.
**When not to use a cloud warehouse:** data that fits on one machine (DuckDB), sub-second
operational lookups (use a KV store / Postgres), millisecond streaming (Flink), or a hard
requirement to stay engine-neutral (an open lakehouse with Iceberg + Trino/Spark).

**No certifications.** This track deliberately has no cert mapping. Exam guides (SnowPro Core
COF-C03, Google Associate Data Practitioner / Professional Data Engineer, Databricks DEA,
Microsoft DP-700) were used only as a coverage checklist, so nothing important is missing.
Product trivia (editions, menu settings, UI clicks) is left out. Databricks exam depth lives in
the separate DEA prep project. CDW-12/13 only cover the platform layer on top of SPK and LKH-06.

Prerequisites: SPK, LKH (Delta in LKH-06, Iceberg in LKH-01..05). Can run in parallel with JVM.
Lab profile: none for local exercises; each platform block has one cloud lab on a free tier or
trial (`☁️free`).

**Platform order:** Snowflake → BigQuery → Databricks → Fabric (job-posting frequency at
product/tech companies; Fabric is strongest in Microsoft-centric enterprises).

## How exercises are graded

The SaaS platforms can't run locally, so each exercise is marked with how it is graded:

| Mark | Mode | Validity gates |
|---|---|---|
| **L** | Local and hermetic: emulator or local engine (`fakesnow`, Snowpark local testing, `bigquery-emulator`, Spark + Delta, Kusto emulator, `sqlglot`) | all of G1–G6 |
| **C** | Cloud lab `☁️free`: you run it on a free tier or trial; `de` collects **evidence** (query history, `EXPLAIN`/query profile JSON, dry-run bytes, grants output) and a local checker grades it | G1, G2, G4 on the checker over recorded evidence fixtures; G5/G6 relaxed |
| **R** | Rubric plus numeric checks with tolerances (ARC style) | rubric + numeric tests |

Free tier windows: the Snowflake and Fabric trials expire, so `de plan` should schedule each
platform's **C** exercises inside one trial window. BigQuery (sandbox / free tier) and
Databricks (Free Edition) have no deadline. Emulator coverage and the trial terms must be
re-checked in the build phase and pinned in `labs/versions.lock`.

## Shared core

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| CDW-01 | Cloud warehouse architecture | 3 | Shared-nothing MPP vs. disaggregated storage/compute; serverless; result/local/remote caches; metadata services; why every vendor ended up on object storage + open formats | **R+L** Map 4 platforms onto one reference model (storage, compute, metadata, cache, catalog); property test on the mapping file | Redshift DC2 vs. RA3 |
| CDW-02 | Pricing & cost models | 3 | Snowflake credits × size × time, auto-suspend; BigQuery on-demand bytes vs. editions/slot autoscaling; DBUs classic vs. serverless; Fabric capacity units, smoothing, throttling | **L** Cost model for the C1 gold workload on all 4 platforms, checked within ±20% | Athena $/TB scanned |
| CDW-03 | The shared feature set | 4 | Time travel, zero-copy clone, data sharing, materialized views, semi-structured types (VARIANT/JSON/STRUCT/ARRAY), RBAC + row/column policies, open-format interop (Snowflake Iceberg tables, BigLake, UniForm, OneLake shortcuts) | **R** Decision records: which platform/feature for 5 scenarios (multi-company sharing, SOX audit, PII masking, ...) | Iceberg snapshots ≈ time travel |

## Snowflake

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| CDW-04 | Architecture & virtual warehouses | 4 | Three layers; micro-partitions and pruning; clustering keys, automatic clustering, search optimization; warehouse sizing, multi-cluster, query acceleration; Query Profile; caches | **C** Make 4 queries prune (evidence: partitions scanned / total from query history); **L** predict pruning from given partition metadata | DISTKEY/SORTKEY → clustering keys |
| CDW-05 | Loading & pipelines | 4 | Stages, file formats, `COPY INTO`, Snowpipe / Snowpipe Streaming; Streams & Tasks; Dynamic Tables (target lag); `MERGE` | **L** Incremental SCD2 from a change table with `MERGE` (fakesnow, DuckDB oracle); **C** Streams + Task pipeline, then the same with a Dynamic Table | dbt incremental models |
| CDW-06 | Security, governance, sharing | 3 | Role hierarchy (system vs. custom roles), future grants, masking and row access policies, tags, secure views, secure data sharing, replication/failover | **C** Multi-company RBAC: each company's analyst role sees only its rows and masked PII; the checker asserts denied queries | SOX access controls; multi-company marts |
| CDW-07 | Snowpark & Iceberg tables | 4 | Snowpark DataFrame vs. PySpark (pushdown to SQL), UDFs/UDTFs, stored procedures; Snowflake-managed vs. externally managed Iceberg tables, catalog integration | **L** Port an SPK-03 transformation to Snowpark (local testing mode), same oracle as SPK-03; **C** read an LKH Iceberg table from Snowflake | Your Iceberg tables in Athena |

## BigQuery

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| CDW-08 | Architecture, slots, pruning | 4 | Dremel, Colossus, Capacitor, shuffle tier; slots; on-demand vs. editions/reservations; partitioning + clustering, `require_partition_filter`; dry runs; `INFORMATION_SCHEMA.JOBS` | **C** Cut bytes billed for 5 queries; the checker asserts dry-run bytes under thresholds (dry runs are free) | Athena partition projection, $/TB |
| CDW-09 | Modeling in BigQuery | 3 | Nested and repeated fields vs. star schema, when to denormalize, `MERGE`, materialized views, search indexes, BI Engine | **L** Rebuild `fct_orders` with nested line items; results match the flat star schema via a DuckDB oracle (bigquery-emulator) | **Kimball vs. one-big-table** |
| CDW-10 | Ingestion & processing on GCP | 3 | Load jobs vs. Storage Write API (committed / pending streams), Pub/Sub BigQuery subscriptions, Datastream CDC, Data Transfer Service; Dataflow/Beam and Dataproc in one lesson each, positioned vs. Flink/Spark | **R** Ingestion design memo for 3 sources; **C** exactly-once load with a Storage Write API committed stream (no duplicates after a retried append) | Loading into Redshift |
| CDW-11 | Governance & lakehouse on GCP | 2 | IAM on datasets, authorized views, row-level security, policy tags (column-level), data masking, BigLake / BigLake Iceberg tables, Analytics Hub | **C** Policy-tag column security + row filter per company; the checker asserts denied reads | SOX, multi-company |

## Databricks (platform layer only; Spark and Delta come from SPK and LKH-06)

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| CDW-12 | Workspace, compute, Unity Catalog | 4 | Clusters vs. SQL warehouses vs. serverless, Photon; Unity Catalog (metastore → catalog → schema, grants, volumes, lineage, external locations), Delta Sharing | **C** (Free Edition) Grants for 2 company groups + lineage evidence for a gold table; **L** Unity Catalog grant model reproduced on OSS Unity Catalog, if it covers grants (check at build time) | Glue catalog + Lake Formation |
| CDW-13 | Lakeflow pipelines & jobs | 4 | Auto Loader, Lakeflow Declarative Pipelines (formerly DLT) with expectations, Jobs and task dependencies, Asset Bundles (CI/CD), liquid clustering, predictive optimization | **C** (Free Edition) Medallion pipeline with expectations; the checker reads the pipeline event log (expectation pass/fail counts) | dbt tests ≈ expectations |

## Microsoft Fabric

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| CDW-14 | Fabric & OneLake | 4 | Capacities, workspaces, items; OneLake (one Delta-based lake per tenant), shortcuts, mirroring; Lakehouse vs. Warehouse vs. Eventhouse; V-Order; Direct Lake | **R** Pick the data store and transform tool for 5 scenarios; **C** (trial) lakehouse with a shortcut to external data + a mirrored source | S3 + Glue + Redshift Spectrum |
| CDW-15 | Fabric data engineering | 4 | Notebooks (Spark), Dataflow Gen2, pipelines (parameters, dynamic expressions, triggers); Warehouse T-SQL (CTAS, `COPY INTO`, `MERGE`); git integration and deployment pipelines | **L** Watermark-based incremental load logic in PySpark + Delta (local Spark, DuckDB oracle); **C** the same as a parameterized pipeline | dbt + scheduler |
| CDW-16 | Real-Time Intelligence & governance | 4 | Eventstream, Eventhouse/KQL databases, KQL windowing and `summarize`; workspace/item/OneLake security, RLS/CLS, sensitivity labels; monitoring hub, capacity metrics | **L** 6 KQL queries (windows, dedup, late data) against the Kusto emulator, checked by a DuckDB oracle; **R** security design for a multi-company workspace | Athena SQL → KQL |

## Closing

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| CDW-17 | One design, four stacks | 3 | SQL dialect differences (`QUALIFY`, `MERGE`, semi-structured access, date functions); dbt adapters; migrating from Redshift; lock-in vs. open formats; how to pick a platform | **L** Port C1's gold SQL to the Snowflake and BigQuery dialects; results must match on the emulators (sqlglot as a transpile baseline, not as the answer) | dbt across adapters |

Interview drills: "Snowflake vs. BigQuery vs. Databricks for a 5 PB/year workload?",
"Why is this BigQuery query expensive?", "How do you isolate tenants in a shared warehouse?",
"Warehouse or lakehouse?".

ARC-05 (platform landscape) builds on this track: it reuses the CDW-02 cost model and the
CDW-17 port instead of re-teaching the platforms.
