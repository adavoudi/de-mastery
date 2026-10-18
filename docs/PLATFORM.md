# Platform design

## Course format

Every module uses the same structure. Each part is graded by the `de` CLI.

| Part | Style | Graded by |
|---|---|---|
| **Lesson** (`lesson.md`) | Short concept text: *Why it exists → How it works → From your world → When NOT to use it* | – |
| **Exercises** (`exercises/NN-*/`) | boot.dev style: small task, starter code, hidden + visible tests | pytest / JUnit |
| **Quiz** (`quiz.yaml`) | Multiple choice. Every question cites a doc source | auto |
| **Explain-it** (`explain.md`) | Write a 200-word explanation as if teaching it (Feynman technique) | self-check rubric |
| **Interview drill** (`drills.md`) | 3–5 questions with model answers | self-graded |

Each core track also has one **build-your-own (BYO) challenge** in CodeCrafters style:

- It is split into 6–9 **stages**. Each stage has its own test suite, and you can only move on when it passes.
- Written in Python, so the learning goes into the concept rather than the language. One stage
  is optionally ported to Java for JVM practice.
- Stage tests talk to your implementation the way a real client would (over a socket, a file
  layout, or a CLI). This means any language works.

### Module metadata (`module.yaml`)

```yaml
id: SPK-06
title: Joins and aggregations at scale
hours: 6
requires: [SPK-05]
labs: [core]
cost: free            # free | 💲 (~$X estimate) | 💲💲
from_your_world: "Redshift DISTKEY/DISTSTYLE ALL ≈ broadcast join"
```

## `de` CLI

| Command | Does |
|---|---|
| `de status` | Passed/total per track and BYO challenge |
| `de next` | Opens the next unfinished exercise, respecting `requires` |
| `de start <ID>` | Jumps to a module or stage, and warns if prerequisites are missing |
| `de test` | Runs the tests for the current exercise. Result is stored in `progress.json` |
| `de hint` | Progressive hints (3 levels) |
| `de solution` | Unlocks after a pass or after 3 failed attempts |
| `de lab up <profile>` / `down` | Starts or stops Docker Compose profiles |
| `de web` | Serves the course as a local website (lessons, exercises, quizzes), see [WEB.md](WEB.md) |
| `de plan` | Recalculates the schedule from `learner.yaml` (see below) |
| `de quiz <ID>` | Module quiz; best score saved |
| `de validate <ID>` | Runs the validity gates on an exercise (see [TESTING.md](TESTING.md)) |
| `de doctor` | Environment check with fixes |

Implementation: Python 3.12, Typer, uv. Progress is stored in `progress.json`, a local, gitignored file. The course repo holds content only; your progress, `workspace/` and `learner.yaml` are never committed.

### Scheduling

`de plan` reads `pace.hours_per_week`, `pace.target_date`, `track_deadlines` and `skip_modules`
from `learner.yaml`, plus the remaining module hours:

- **No target date:** it projects a finish date from your pace.
- **Target date set:** it computes the hours/week needed. If that is more than your pace, it
  says so and suggests what to skip or reorder.
- **Re-planning:** past progress is never lost. Changing pace or date only changes the projection
  for remaining work. Your measured hours per module also refine later estimates.

## Repository layout

```
de-mastery/
├── README.md  ROADMAP.md  learner.example.yaml
│   (local, gitignored: learner.yaml  progress.json  workspace/)
├── docs/              # this file, TUTORIAL.md, TESTING.md, WEB.md, DIAGRAMS.md
├── tracks/            # one overview per track
├── cli/               # `de` source
├── labs/
│   ├── compose.yaml   # profiles: core, streaming, flink, streamhouse, observability
│   ├── versions.lock
│   └── data/          # synthetic data generators (commerce, multi-company)
├── content/
│   └── SPK/SPK-06/
│       ├── module.yaml  lesson.md  quiz.yaml  explain.md  drills.md
│       ├── diagrams/
│       └── exercises/01-broadcast-join/
│           ├── starter/  tests/  solution/  mutants/
└── byo/
    └── mini-kafka/stage-01/ ... stage-08/
```

## Local lab stack (32 GB RAM)

Profiles keep the memory footprint bounded. Run one or two at a time.

| Profile | Services | Approx. RAM |
|---|---|---|
| `core` | MinIO (S3), Iceberg REST catalog, Spark (driver + 2 workers), Jupyter, Trino | 8–10 GB |
| `streaming` | Kafka (KRaft, 3 brokers), Schema Registry, Kafka Connect + Debezium, Postgres (OLTP source), Kafka UI | 6–8 GB |
| `flink` | Flink JobManager + 2 TaskManagers, SQL Gateway | 4–6 GB |
| `streamhouse` | `flink` + Paimon + Fluss + StarRocks | 8–10 GB |
| `observability` | Prometheus + Grafana with dashboards for Spark, Kafka and Flink | 1–2 GB |

> **Kafka and Java exercises grade without Docker.** The `streaming` profile above
> is an *optional* extra layer (e.g. Debezium CDC). Most Kafka/Java exercises run
> against **native** runtimes vendored under gitignored `labs/` dirs — Apache
> Kafka 4.3.1 (`bash labs/fetch_kafka.sh`), the JVM/JUnit jars
> (`bash labs/fetch_java_deps.sh`), Gradle for JVM-03 (`bash labs/fetch_gradle.sh`)
> and an optional native Schema Registry (`bash labs/fetch_schema_registry.sh`),
> all on the vendored JDK 21. `de doctor` reports each. The de_testkit
> `kafka_broker`/`kafka_cluster`/`run_junit` fixtures use them and skip cleanly
> if absent. The `streaming` lab binds non-standard host ports (Kafka 19092,
> Schema Registry 8081, Connect 8083, CDC Postgres **5433**, UI 8085), never
> 5432/8888. See `labs/README.md`.

Synthetic data: a seeded generator produces a multi-company commerce dataset (orders, payments,
refunds, customers, products, FX rates, and company acquisitions that happen mid-period).
It scales from 1 GB to ~200 GB locally, so you can run **scaling experiments**: measure how runtime,
shuffle size and file counts grow from 1 → 10 → 100 GB, then extrapolate to PB with a cost model.

## Cost flags

| Flag | Meaning | Example |
|---|---|---|
| (none) | Runs locally, free | most exercises |
| `☁️free` | Uses a free tier or trial; no card charge expected | Databricks Free Edition, Confluent Cloud trial |
| `💲` | Under ~$5 per run on AWS | EMR Serverless Spark job over 50 GB on S3 |
| `💲💲` | $5–25 per run; optional unless marked "core" | 1 TB EMR scaling run, MSK Serverless for one evening |

Rules for every 💲 lab:

- It shows its estimate before it starts.
- It tags resources with `project=de-mastery`.
- It ships a `teardown` script that the `de` CLI runs automatically when the time limit expires.

For actual prices, check the AWS Pricing Calculator. Estimates here are rough guides only.
