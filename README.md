# DE Mastery: from Analytics Engineer to Data Platform Engineer

A self-paced learning platform for Spark, Kafka, Flink, open table formats (lakehouse) and
streamhouse architectures. It is written for analytics engineers and other SQL-first data people
who know dbt, Kimball modeling and a cloud warehouse, and want to move into data platform
engineering. Each module connects new ideas to those familiar ones.

It uses two course formats together:

- **boot.dev style**: short lessons, then exercises graded by automated tests.
- **CodeCrafters style**: "build your own X" challenges split into stages, which teach the
  internals that interviewers ask about.

> Status: the `de` CLI, the `core`, `streaming`, `flink` and `streamhouse` labs, and the FND, SPK,
> LKH, JVM, KFK, FLK and STH tracks (each with a build-your-own challenge) plus the C3 streamhouse
> capstone scaffolding are built. Every exercise passes the validity gates G1-G6 (see
> [docs/TESTING.md](docs/TESTING.md)). Graded exercises run natively with no Docker: Kafka, Flink
> 2.2.1, Apache Paimon 2.0.0 and Apache Fluss 1.0.0 are downloaded by the `labs/fetch_*.sh`
> scripts (run them once; see [labs/README.md](labs/README.md)). The optional `flink` and
> `streamhouse` Docker lab profiles are extra layers. Mock exams are planned next. See
> [ROADMAP.md](ROADMAP.md) → "Status".

## Files

| File | Purpose |
|---|---|
| **[docs/TUTORIAL.md](docs/TUTORIAL.md)** | **Start here:** how to study with this repo (reading order, exercise loop, BYO, weekly rhythm) |
| [ROADMAP.md](ROADMAP.md) | Tracks, order, dependency graph, how existing skills map to new ones, build phases |
| [learner.example.yaml](learner.example.yaml) | Template for your profile (copy to `learner.yaml`, which is gitignored): pace, target dates, cert choices, cost budget |
| [docs/SERVER.md](docs/SERVER.md) | The optional progress server: `de login` / `de sync`, dashboard, how to host it |
| [docs/PLATFORM.md](docs/PLATFORM.md) | Course format, `de` CLI, repo layout, local lab stack, cost flags |
| [docs/TESTING.md](docs/TESTING.md) | How we make sure the exercise tests are valid |
| [tracks/](tracks/) | One syllabus per track (FND, SPK, LKH, CDW, JVM, KFK, FLK, STH, ARC) |
| [capstones/README.md](capstones/README.md) | Track projects + 3 capstones and what each covers |
| [certs/README.md](certs/README.md) | Certification exams, verified objectives, mapping to modules |

## Principles

1. **Philosophy before API.** Every track begins with *why the tool exists* and *when not to use it*.
2. **Connect to what you know.** Each module includes a "From your world" section that maps it
   to dbt, Redshift/Athena, Iceberg, Kimball or SOX.
3. **Tests are the teacher.** Exercises pass or fail against tests that have been proven valid
   (see TESTING.md).
4. **Build the internals once.** One "build your own" challenge per core tool.
5. **Scale honestly.** Labs run locally on 32 GB of RAM. Reasoning at petabyte scale is taught
   through measured scaling experiments, cost models and design reviews. Large runs on AWS are
   optional and flagged `💲`.
6. **Certificates are a by-product.** Modules map to official exam objectives, so the exams come
   at the end of the work, not as a separate effort.

## Getting started

The tooling (the `de` CLI, test kit and data generator) is a single [uv](https://docs.astral.sh/uv/)
project at the repo root. Python 3.12.

```bash
# 1. Install dependencies into a local .venv (pins come from uv.lock)
uv sync

# 2. Check your environment (python, uv, java, docker, RAM) and get fixes
uv run de doctor

# 3. See what to work on next
uv run de next
```

New here? Read [docs/TUTORIAL.md](docs/TUTORIAL.md) first.

Notes:

- **Java for Spark.** Spark 4.x needs Java 17+. If `de doctor` says your Java is not an
  LTS/recommended runtime (or none is found), run `bash labs/setup_jdk.sh` to install a
  vendored Eclipse Temurin **JDK 21** into `labs/.jdk/` (no sudo, no system changes). The CLI
  and the test `spark` fixture detect it automatically.
- **Running the tool's own tests:** `uv run pytest cli/tests -q`.
- **Validating exercise content:** `uv run de validate <MODULE_or_EXERCISE> [--content-root DIR]`.
- `de` commands you can use now: `status`, `next`, `start`, `test`, `hint`, `solution`,
  `validate`, `plan`, `quiz`, `doctor`, and `lab up|down|status <profile>`.
  `mock`, `review`, `cost` and `dispute` are stubs that print which later phase adds them.
