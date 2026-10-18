# Local lab stack

Docker Compose profiles that back the exercises. Phase 1 ships the **`core`**
profile (lakehouse: object store + Iceberg catalog + Spark + Jupyter + Trino).
Phase 3 adds the **`streaming`** profile (see below). Later phases add `flink`,
`streamhouse` and `observability` as new services in the same `compose.yaml`.

Everything binds to **127.0.0.1 only** and uses local-only dev credentials from
`labs/.env`. Nothing here is reachable from off your machine.

## native runtimes (no Docker) + the `streaming` profile

Most Phase 3 (Kafka/Java) grading runs **without Docker**, from vendored,
checksum-verified runtimes under gitignored `labs/` dirs. Run these once (and
`de doctor` reports each):

```bash
bash labs/setup_jdk.sh             # Temurin JDK 21 -> labs/.jdk (shared w/ Spark)
bash labs/fetch_kafka.sh           # Apache Kafka 4.3.1 (Scala 2.13) -> labs/.kafka   [sha512]
bash labs/fetch_java_deps.sh       # JUnit/AssertJ/Kafka/Avro jars -> labs/.jars/java [sha256]
bash labs/fetch_gradle.sh          # Gradle 8.14.3 -> labs/.gradle-dist [sha256; JVM-03 only]
bash labs/fetch_schema_registry.sh # OPTIONAL native Confluent SR -> labs/.schema-registry [sha512]
```

- The de_testkit `kafka_broker` / `kafka_cluster` fixtures start real KRaft
  brokers from `labs/.kafka` on JDK 21 (random free ports, `-Xmx512m`). They
  **SKIP cleanly** if the tarball/JDK is missing.
- Java exercises compile + run via `run_junit` (javac + JUnit console launcher);
  Kafka Streams uses `TopologyTestDriver` (no broker). Gradle is only for JVM-03.
- Schema compatibility (KFK-06) is graded with `de_testkit.check_compatibility`
  (fastavro) — **no registry process needed**. A live registry is optional.

The **`streaming`** Docker profile is an *extra* layer (e.g. Debezium CDC):

```bash
cd labs
docker compose --profile streaming up -d        # or: de lab up streaming
bash smoke_streaming.sh                          # end-to-end CDC smoke test
bash smoke_streaming.sh --keep-up                # leave it running
docker compose --profile streaming down -v       # or: de lab down streaming
```

| Service | Image | Host port (127.0.0.1) | Mem limit |
|---|---|---|---|
| `kafka` | apache/kafka:4.3.1 (KRaft) | 19092 | 1.2 GB |
| `schema-registry` | confluentinc/cp-schema-registry:8.0.8 | 8081 | 0.8 GB |
| `connect` | debezium/connect:3.0.0.Final (Debezium Postgres) | 8083 | 1.2 GB |
| `cdc-postgres` | postgres:16.6 (`wal_level=logical`) | **5433** (NOT 5432) | 0.5 GB |
| `kafka-ui` | kafbat/kafka-ui:v1.1.0 | 8085 | 0.5 GB |

Ports are deliberately non-standard and bind 127.0.0.1, so they never clash with
a local Postgres (5432) or pgAdmin (8888). `de lab up|down|status
streaming` wraps Compose. **Do not run `de lab down`** if another session may be
using a lab.

## native Flink (no Docker) + the `flink` profile

Most Phase 4 (Flink / FLK track) grading runs **without Docker**, from a
vendored, checksum-verified Flink 2.x distribution under gitignored `labs/`.
Run this once (and `de doctor` reports it):

```bash
bash labs/setup_jdk.sh    # Temurin JDK 21 -> labs/.jdk (shared w/ Spark/Kafka)
bash labs/fetch_flink.sh  # Flink 2.2.1 dist -> labs/.flink [sha512]; connector +
                          # Iceberg + hadoop jars -> labs/.jars/flink [sha256];
                          # and the isolated PyFlink venv -> labs/.flink/.pyflink-venv
```

- **Flink 2.2.1 (Scala 2.12)** — the newest 2.x line with a released
  `flink-connector-kafka` (`5.0.0-2.2`). The de_testkit `flink_java` /
  `flink_sql` / `flink_cluster` fixtures run embedded jobs and a native local
  cluster (JobManager + TaskManagers on free ports, small heaps) straight from
  `labs/.flink` on JDK 21. They **SKIP cleanly** if the dist/JDK is missing.
- **PyFlink** (`apache-flink==2.2.1`) lives in a **dedicated venv**
  (`labs/.flink/.pyflink-venv`), NOT in `pyproject.toml` — its `apache-beam`
  dependency caps `pyarrow`/`numpy` below the main project's pins. The
  `pyflink_runner` fixture shells out to that interpreter; FLK-08's Python side
  SKIPs if the venv is absent.
- **Iceberg on Flink** uses `iceberg-flink-runtime-2.2:1.12.0` (Iceberg 1.10.0
  has no Flink-2.2 runtime — see `versions.lock` → `iceberg_flink_decision`).

The **`flink`** Docker profile is an *optional* extra layer (deploy/operate
demos, FLK-01/12):

```bash
cd labs
docker compose --profile flink up -d             # or: de lab up flink
bash smoke_flink.sh                               # end-to-end: submit WordCount
bash smoke_flink.sh --keep-up                     # leave it running
docker compose --profile flink down -v            # or: de lab down flink
```

| Service | Image | Host port (127.0.0.1) | Mem limit |
|---|---|---|---|
| `flink-jobmanager` | flink:2.2.1-scala_2.12-java17 | 8086 (REST/UI) | 1.5 GB |
| `flink-taskmanager` | flink:2.2.1-scala_2.12-java17 | – | 2.0 GB |
| `flink-sql-gateway` | flink:2.2.1-scala_2.12-java17 | 8087 (REST) | 1.0 GB |

Ports are deliberately non-standard and bind 127.0.0.1, so they never clash with
a local Postgres (5432) / pgAdmin (8888) or the other profiles. `de lab up|down|
status flink` wraps Compose. **Do not run `de lab down`** if another session may be
using a lab. (On a 32 GB host, run ONE lab profile at a time — the native,
no-Docker path is what the graded tests use.)

## native streamhouse (no Docker) + the `streamhouse` profile

The Phase 5 (STH track: Apache Paimon, Apache Fluss, Iceberg-as-streaming-storage)
grading runs **without Docker**, reusing the vendored Flink 2.2.1 dist + JDK 21
and adding the native Fluss server + Paimon/Fluss jars.

```bash
bash labs/fetch_flink.sh         # prerequisite (Flink 2.2.1 + Iceberg/hadoop jars)
bash labs/fetch_streamhouse.sh   # Fluss 1.0.0 server -> labs/.fluss [sha512];
                                 # Paimon 2.0.0 + Fluss 1.0.0 Flink jars ->
                                 # labs/.jars/streamhouse [sha256]; builds the
                                 # isolated pypaimon venv -> labs/.streamhouse
bash labs/smoke_streamhouse.sh   # native end-to-end smoke (NO Docker)
bash labs/smoke_streamhouse.sh --with-fluss   # also start+stop a Fluss cluster
uv run de doctor                 # must show `streamhouse (native)` OK
```

- **Apache Paimon 2.0.0** — the `paimon_sql` fixture runs embedded Flink SQL on an
  isolated tmp warehouse (`batch` + bounded `streaming` with a deadline that
  cancels the job). Partial-update / changelog / merge semantics.
- **Apache Fluss 1.0.0** — the `fluss_cluster` fixture starts a native ZK +
  coordinator + 1 tablet server on OS-assigned free ports (small heaps, isolated
  dirs, guaranteed teardown), optionally with Fluss→Paimon lake tiering.
- **Iceberg-as-streaming-storage** — the `iceberg_sql` fixture reuses
  `iceberg-flink-runtime-2.2:1.12.0`; a bounded debezium-json source → Iceberg
  sink commits on checkpoint, then a batch read reconciles.
- **Serving** — `serving_query` reads Paimon via the isolated pypaimon venv and
  Iceberg via pyiceberg → Arrow → DuckDB. Pure Python, no Docker.
- All fixtures **SKIP cleanly** if an artifact or the JDK is missing.

The **`streamhouse`** Docker profile is an *optional* extra layer (a Trino serving
engine over the lakehouse tables) and is **not needed for grading**:

```bash
docker compose --profile streamhouse up -d       # or: de lab up streamhouse
bash smoke_streamhouse.sh                          # the NATIVE path (needs NO Docker)
docker compose --profile streamhouse down -v       # or: de lab down streamhouse
```

| Service | Image | Host port | Memory |
|---|---|---|---|
| `streamhouse-serving` | trinodb/trino:476 | 8089 | 1.5 GB |

Ports bind 127.0.0.1 and are non-standard (never 5432/8888). `de lab up|down|
status streamhouse` wraps Compose. **Do not run `de lab down`** if another session
may be using a lab. On 32 GB, the native path (no Docker) is what the graded STH
tests use.

## Quick start

```bash
cd labs
cp .env.example .env            # local-only dev credentials; .env is gitignored

# Build + start the core profile
docker compose --env-file .env --profile core up -d --build

# Prove it works end to end (Spark writes an Iceberg table, Trino + Spark read
# it, MERGE INTO, snapshot check). Exits non-zero on any failure.
bash smoke_core.sh              # leaves the stack up
bash smoke_core.sh --down       # run, then tear down (removes volumes)
bash smoke_core.sh --down-only  # just tear everything down

# Stop
docker compose --env-file .env --profile core down -v
```

First `up` builds the Spark image (bakes in the Iceberg jars) and pulls the
other images — allow a few minutes. Later starts are fast.

## Services (profile `core`)

| Service | Image | Host port (127.0.0.1) | What it is | Mem limit |
|---|---|---|---|---|
| `silo` | pgsty/silo | 9000 (S3), 9001 (console) | S3-compatible object store (a maintained MinIO fork). Holds the Iceberg warehouse. | 1.5 GB |
| `create-buckets` | pgsty/mc | – | One-shot: creates the `warehouse` bucket, then exits. | – |
| `iceberg-rest` | apache/iceberg-rest-fixture | 8181 | Iceberg REST catalog; data + metadata written to `silo`. | 1.0 GB |
| `spark` | built from apache/spark:4.0.0 | 15002 (Connect), 4040 (UI) | Spark 4.0.0 in `local[*]`, exposing a **Spark Connect** server. Iceberg jars baked in. | 3.5 GB |
| `jupyter` | quay.io/jupyter/pyspark-notebook | 8889 → 8888 | JupyterLab with PySpark 4.0.0; a thin **Connect client** to `spark`. | 1.5 GB |
| `trino` | trinodb/trino:476 | 8080 | SQL engine with an `iceberg` catalog over the same REST catalog. | 2.0 GB |

Exact tags, digests and the Iceberg jar coordinates are in
[`versions.lock`](versions.lock).

### RAM

Memory **limits** total **9.5 GB** (bounded under ~10 GB as required). Observed
**peak** during the smoke test (a light workload) was ~**2.3 GB** across all
core containers. Real Spark jobs push `spark`/`trino` toward their limits, but
the limits keep the whole profile under 10 GB. On 32 GB you can run `core` plus
one other profile.

## Connecting from the host

### PySpark via Spark Connect (recommended)

The lab exposes a **Spark Connect** server, so a notebook or script on your host
talks to Spark over gRPC — **no local Java and no Spark JARs needed** (important
here: the host JDK is 24, which Spark/Hadoop do not support; the server runs
JDK 17 inside the container).

```python
# pip install "pyspark[connect]==4.0.0"
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.sql("SHOW NAMESPACES IN demo").show()      # `demo` = the Iceberg REST catalog
spark.sql("SELECT * FROM demo.smoke.orders").show()
```

The Iceberg catalog is pre-wired on the server as **`demo`** (REST → silo/S3),
so you do not pass any `--packages` or catalog config from the client.

### JupyterLab

Open <http://localhost:8889> (token is empty by default; set `JUPYTER_TOKEN` in
`.env` to require one). In a notebook, connect to the server at
`sc://spark:15002` (the service name on the compose network):

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://spark:15002").getOrCreate()
```

### Trino

- Web UI / REST: <http://localhost:8080>
- CLI: `docker compose --profile core exec trino trino --catalog iceberg`
- JDBC: `jdbc:trino://localhost:8080` (user: any; no password)

```sql
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.smoke.orders;
```

### Object store console

<http://localhost:9001> — log in with the `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` from your `.env`. Browse `warehouse/` to see the Parquet
data files and the Iceberg `metadata/` tree.

## How the pieces fit

```
  host PySpark / Jupyter ──gRPC──► spark (Connect server, local[*])
                                     │  reads/writes
   Trino ──REST──► iceberg-rest ◄────┘  tables
                        │   (catalog: "which metadata file is current?")
                        ▼
                      silo (S3)   data files + metadata.json under s3://warehouse/
```

Both engines use the **same** REST catalog, so a table written by Spark is
immediately queryable from Trino and vice-versa.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `port is already allocated` on 8888/8080/9000… | Another app holds the host port (e.g. a local pgadmin on 8888). | Change the port in `.env` (Jupyter already defaults to **8889**). |
| `spark` never becomes healthy | First start downloads nothing (jars are baked) but the JVM takes ~30-60 s. | Wait; check `docker compose --profile core logs spark`. Health probes the UI on :4040. |
| `pull access denied` for `minio/minio` | Upstream MinIO images were removed from Docker Hub / gated on quay. | This lab uses `pgsty/silo` instead; no action needed. See `versions.lock` notes. |
| `ModuleNotFoundError: pyspark` in a host script | `pyspark[connect]` not installed, or wrong version. | `pip install "pyspark[connect]==4.0.0"`. |
| `NoSuchMethodError` / `ClassNotFoundException: S3FileIO` from Spark | Iceberg jar mismatch (should not happen — jars are baked). | Rebuild: `docker compose --profile core build spark`. |
| Trino: `Schema 'x' does not exist` | Table not created yet, or wrong catalog. | Create it from Spark first; query as `iceberg.<schema>.<table>`. |
| `create-buckets` loops on "waiting for silo" | Client binary name. | It must call `mc` (not `mcli`); already fixed in compose. |
| Bucket missing / writes go nowhere | `create-buckets` didn't finish before an engine started. | It's a dependency gate; re-run `up`. Check `docker logs de-create-buckets` for `BUCKET_READY`. |

## Design notes

- **Why a MinIO fork (`pgsty/silo`)** instead of `minio/minio`: upstream MinIO
  ended community image distribution (Docker Hub repos deleted, quay.io gated
  behind auth), so `minio/minio` is no longer pullable. `silo` is a drop-in fork
  that keeps the S3 API, `MINIO_*` env vars and the console. Full rationale in
  [`versions.lock`](versions.lock).
- **Why single-container Spark + Spark Connect** instead of master + 2 workers:
  far fewer moving parts (no worker-registration races), lower memory, and it
  matches the SPK-01 syllabus (Spark Connect, client/server split). The local
  scheduler still produces real jobs, stages, tasks, shuffles and a UI.
- **Why the Iceberg jars are baked into the image**: resolving `--packages` at
  startup needs a writable ivy cache, which failed on a fresh named volume and
  stopped the Connect server. Baking the two jars in removes all runtime
  resolution — the server, `spark-sql` and PySpark all just work offline.
