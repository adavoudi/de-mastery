"""de_testkit pytest plugin.

Registered via the ``pytest11`` entry point so every exercise test suite can use
these fixtures and helpers without any imports or ``conftest.py``.

SESSION MODEL (important — read before adding Spark fixtures)
-------------------------------------------------------------
Spark allows exactly ONE active SparkContext per JVM/process. ``spark.jars`` and
``spark.sql.extensions`` are fixed when that context starts, so a second
``getOrCreate()`` with different JARs is silently ignored. To make the plain
``spark`` fixture and the Iceberg/Delta ``spark_iceberg`` fixture coexist inside a
single pytest process, both share ONE session built by ``_get_or_build_session``:

* If the pinned Iceberg + Delta JARs are present (``labs/fetch_jars.sh``) and Java
  is usable, the shared session is built WITH the Iceberg + Delta SQL extensions
  and a Hadoop-backed Iceberg catalog named ``local`` rooted at a per-session tmp
  warehouse. The extensions are inert for ordinary DataFrame ops, so existing
  (FND/SPK) content that only uses ``spark`` is unaffected — verified against the
  SPK plan-assertion exercises.
* If the JARs are missing, the shared session is built bare (``local[2]`` only).
  ``spark`` still works for no-Docker Python/Spark exercises; ``spark_iceberg`` /
  ``delta_spark`` SKIP with a clear message pointing at ``labs/fetch_jars.sh``.

Because both fixtures return the SAME object, they can never fight over JVM-level
config within one process. Content tests may use either in any file.
"""

from __future__ import annotations

import os

import pytest

from de_testkit import jars as jars_mod
from de_testkit import java_env
from de_testkit.asserts import (
    assert_frames_equal,
    assert_plan_contains,
    count_exchanges,
    plan_of,
)
from de_testkit.streaming import FileStreamSource, drive_available
from de_testkit.submission import import_submission

__all__ = [
    "submission",
    "spark",
    "spark_iceberg",
    "delta_spark",
    "pyiceberg_catalog",
    "lab_spark",
    "trino",
    "duckdb_con",
    "commerce_data",
    "assert_frames_equal",
    "assert_plan_contains",
    "count_exchanges",
    "plan_of",
    "FileStreamSource",
    "drive_available",
    # Phase 3 (Kafka / Java)
    "kafka_broker",
    "kafka_cluster",
    "schema_registry",
    "check_avro_compatibility",
    "java_toolchain",
    "run_junit",
    "lab_kafka",
    "lab_schema_registry",
    "lab_connect",
    "lab_postgres",
    # Phase 4 (Flink)
    "flink_java",
    "flink_sql",
    "flink_cluster",
    "pyflink_runner",
    "lab_flink",
    # Phase 5 (streamhouse: Paimon / Fluss / Iceberg-as-streaming-storage)
    "paimon_sql",
    "iceberg_sql",
    "fluss_cluster",
    "serving_query",
    "sth_oracle",
    "lab_streamhouse",
]


# ---------------------------------------------------------------------------
# submission
# ---------------------------------------------------------------------------
@pytest.fixture
def submission():
    """Import and return the learner's module from ``DE_SUBMISSION_DIR`` (fresh)."""
    return import_submission()


# ---------------------------------------------------------------------------
# Shared SparkSession factory (one per pytest process)
# ---------------------------------------------------------------------------
# Module-level singletons so `spark` and `spark_iceberg` return the SAME session.
_SESSION = None            # the active SparkSession (or None)
_SESSION_WAREHOUSE = None  # Path to the per-session Iceberg warehouse (or None)
_HAS_TABLE_FORMATS = False # True if the session has Iceberg + Delta extensions


def _ensure_java_on_env() -> dict:
    info = java_env.resolve()
    if info["usable"]:
        os.environ["JAVA_HOME"] = info["java_home"]
        os.environ["PATH"] = (
            os.path.join(info["java_home"], "bin")
            + os.pathsep + os.environ.get("PATH", "")
        )
    return info


def _get_or_build_session(tmp_path_factory):
    """Build (once) and return the shared SparkSession plus a flag indicating
    whether Iceberg + Delta table formats are enabled on it."""
    global _SESSION, _SESSION_WAREHOUSE, _HAS_TABLE_FORMATS
    if _SESSION is not None:
        return _SESSION, _HAS_TABLE_FORMATS

    info = _ensure_java_on_env()
    if not info["usable"]:
        pytest.skip(f"Spark unavailable: {info['reason']}")

    try:
        from pyspark.sql import SparkSession
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"pyspark import failed: {exc}")

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("de-testkit")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.extraJavaOptions",
                "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.executor.extraJavaOptions",
                "-Dio.netty.tryReflectionSetAccessible=true")
    )

    classpath = jars_mod.jars_classpath(jars_mod.ALL_JARS)
    has_formats = classpath is not None
    if has_formats:
        warehouse = tmp_path_factory.mktemp("iceberg_wh")
        builder = (
            builder.config("spark.jars", classpath)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions,"
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            # Iceberg catalog `local` (Hadoop tables rooted in a tmp warehouse).
            .config("spark.sql.catalog.local",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", str(warehouse))
            # Delta catalog wiring (for LKH-06).
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        _SESSION_WAREHOUSE = warehouse

    try:
        session = builder.getOrCreate()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Could not start a SparkSession (Java issue?): {exc}")

    session.sparkContext.setLogLevel("ERROR")
    _SESSION = session
    _HAS_TABLE_FORMATS = has_formats
    return _SESSION, _HAS_TABLE_FORMATS


@pytest.fixture(scope="session")
def _shared_spark(tmp_path_factory):
    session, has_formats = _get_or_build_session(tmp_path_factory)
    yield session, has_formats
    # Stop the session exactly once, at process teardown.
    global _SESSION
    if _SESSION is not None:
        try:
            _SESSION.stop()
        finally:
            _SESSION = None


# ---------------------------------------------------------------------------
# spark (bare local session; skips cleanly if Java is unusable)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark(_shared_spark):
    """Local SparkSession (``local[2]``, shuffle.partitions=4, AQE off, UTC).

    Enough for all pure-Spark (FND/SPK) exercises. If the Iceberg/Delta JARs are
    present it is the same enriched session returned by ``spark_iceberg``; the
    extensions are inert for ordinary DataFrame ops.
    """
    session, _ = _shared_spark
    return session


# ---------------------------------------------------------------------------
# spark_iceberg (Iceberg SQL extensions + `local` catalog)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark_iceberg(_shared_spark):
    """SparkSession with Iceberg SQL extensions and a Hadoop catalog ``local``
    rooted in a per-session tmp warehouse.

    Usage::

        def test_merge(spark_iceberg):
            spark_iceberg.sql("CREATE TABLE local.db.t (id INT) USING iceberg")
            spark_iceberg.sql("INSERT INTO local.db.t VALUES (1)")

    SKIPS cleanly (not fails) when the pinned JARs are missing (run
    ``bash labs/fetch_jars.sh``) or Java is unusable.
    """
    session, has_formats = _shared_spark
    if not has_formats:
        pytest.skip(
            "Iceberg JARs not found. Run `bash labs/fetch_jars.sh` to install "
            "the pinned, sha256-verified runtime JARs into labs/.jars/."
        )
    return session


# ---------------------------------------------------------------------------
# delta_spark (Delta SQL extensions; LKH-06)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def delta_spark(_shared_spark):
    """SparkSession with the Delta Lake SQL extensions and ``DeltaCatalog``.

    Same object as ``spark`` / ``spark_iceberg``; this name documents intent for
    Delta exercises (LKH-06). Write with ``.format("delta")`` and inspect history
    via ``delta.tables.DeltaTable``. SKIPS cleanly when the Delta JARs are missing.
    """
    session, has_formats = _shared_spark
    if not has_formats:
        pytest.skip(
            "Delta JARs not found. Run `bash labs/fetch_jars.sh` to install the "
            "pinned, sha256-verified runtime JARs into labs/.jars/."
        )
    return session


# ---------------------------------------------------------------------------
# pyiceberg_catalog (pure-Python SqlCatalog on sqlite; NO JVM)
# ---------------------------------------------------------------------------
@pytest.fixture
def pyiceberg_catalog(tmp_path):
    """A pyiceberg ``SqlCatalog`` backed by sqlite in ``tmp_path`` (no JVM, no
    Docker). Good for LKH-01 metadata walks and create/append/scan exercises.

    Returns the catalog; a ``warehouse`` directory already exists under
    ``tmp_path/warehouse``. Example::

        def test_scan(pyiceberg_catalog):
            cat = pyiceberg_catalog
            cat.create_namespace("db")
            tbl = cat.create_table("db.t", schema=...)
            tbl.append(arrow_table)
            assert tbl.scan().to_arrow().num_rows == ...
    """
    try:
        from pyiceberg.catalog.sql import SqlCatalog
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"pyiceberg not available: {exc}")
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "local",
        **{
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    return catalog


# ---------------------------------------------------------------------------
# lab_spark / trino (target the `core` Docker lab; SKIP cleanly if it is down)
# ---------------------------------------------------------------------------
def _tcp_open(host: str, port: int, timeout: float = 1.0) -> bool:
    import socket
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def lab_spark():
    """A Spark Connect client pointed at the `core` lab (``sc://localhost:15002``).

    SKIPS cleanly when the lab is not up (nothing listening on the Connect port).
    Only for exercises explicitly marked ``lab: core`` — most exercises must run
    with no Docker. The lab pre-wires the Iceberg REST catalog as ``demo``.
    """
    host = os.environ.get("DE_LAB_SPARK_HOST", "localhost")
    port = int(os.environ.get("DE_LAB_SPARK_PORT", "15002"))
    if not _tcp_open(host, port):
        pytest.skip(
            f"core lab Spark Connect not reachable at {host}:{port}. "
            "Start it with `de lab up core` (requires Docker). Do NOT run "
            "`de lab down` — other sessions may be using it."
        )
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"pyspark import failed: {exc}")
    session = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def trino():
    """A DBAPI connection to the `core` lab Trino (``localhost:8080``).

    SKIPS cleanly when the lab is not up. Requires the ``trino`` python client
    only when actually used; if it is not installed the fixture skips.
    """
    host = os.environ.get("DE_LAB_TRINO_HOST", "localhost")
    port = int(os.environ.get("DE_LAB_TRINO_PORT", "8080"))
    if not _tcp_open(host, port):
        pytest.skip(
            f"core lab Trino not reachable at {host}:{port}. "
            "Start it with `de lab up core` (requires Docker)."
        )
    try:
        import trino as trino_client  # type: ignore
    except Exception:
        pytest.skip("python `trino` client not installed (optional dependency).")
    conn = trino_client.dbapi.connect(host=host, port=port, user="de", catalog="iceberg")
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# duckdb_con (in-memory oracle engine)
# ---------------------------------------------------------------------------
@pytest.fixture
def duckdb_con():
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        yield con
    finally:
        con.close()


# ---------------------------------------------------------------------------
# commerce_data (small seeded dataset -> Parquet + CSV in tmp_path)
# ---------------------------------------------------------------------------
@pytest.fixture
def commerce_data(tmp_path):
    """Generate a small seeded commerce dataset and return a dict with paths.

    Returns ``{"dir": Path, "parquet": {name: Path}, "csv": {...}, "seed",
    "scale"}``.
    """
    from de_data.commerce import generate

    seed = 42
    scale = 0.001
    out = tmp_path / "commerce"
    parquet = generate(seed=seed, scale=scale, out_dir=out)
    return {
        "dir": out,
        "parquet": parquet,
        "csv": {name: p.with_suffix(".csv") for name, p in parquet.items()},
        "seed": seed,
        "scale": scale,
    }


# ===========================================================================
# Kafka fixtures (native, no Docker; vendored tarball + JDK 21)
# ===========================================================================
@pytest.fixture(scope="session")
def kafka_broker():
    """A session-scoped single-node KRaft broker (combined broker+controller)
    started natively from ``labs/.kafka`` on OS-assigned free ports with a small
    heap. Returns a ``KafkaCluster`` with:

      * ``.bootstrap_servers`` — ``127.0.0.1:<port>``
      * ``.admin()`` — a confluent_kafka ``AdminClient``
      * ``.create_topic(name, partitions=, replication_factor=, config=)``
      * ``.run_tool("kafka-topics.sh", "--list", ...)`` — any bundled CLI tool

    SKIPS cleanly (never fails) when the tarball or Java is missing (run
    ``bash labs/fetch_kafka.sh`` / ``bash labs/setup_jdk.sh``). Example::

        def test_roundtrip(kafka_broker):
            from confluent_kafka import Producer, Consumer
            kafka_broker.create_topic("t", partitions=1)
            p = Producer({"bootstrap.servers": kafka_broker.bootstrap_servers})
            p.produce("t", b"hi"); p.flush(10)
    """
    from de_testkit import kafka_support as ks

    ok, reason = ks.kafka_available()
    if not ok:
        pytest.skip(reason)
    cluster = ks.start_single_broker()
    try:
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture
def kafka_cluster():
    """Function-scoped FACTORY for multi-broker clusters with fault injection.

    Call it to build a cluster; every cluster you create is torn down (clean,
    no orphan JVMs) at the end of the test::

        def test_kill_a_broker(kafka_cluster):
            c = kafka_cluster(n_brokers=3)
            c.create_topic("t", partitions=6, replication_factor=3)
            c.kill_broker(2)            # SIGKILL — simulate a crash
            assert c.num_alive() == 2   # cluster still serves
            c.start_broker(2)           # rejoins (same data dir)

    Signature: ``kafka_cluster(n_brokers=3, broker_overrides=None,
    extra_security_protocols=None)``. ``broker_overrides`` merges into every
    broker's ``server.properties`` (e.g. ``{"min.insync.replicas": 2}``);
    ``extra_security_protocols`` adds listeners (e.g.
    ``{"SASL": "SASL_PLAINTEXT"}``) for SASL/SCRAM+ACL exercises (KFK-12).
    SKIPS cleanly when the tarball or Java is missing.
    """
    from de_testkit import kafka_support as ks

    ok, reason = ks.kafka_available()
    if not ok:
        pytest.skip(reason)

    created = []

    def _make(n_brokers: int = 3, *, broker_overrides=None,
              extra_security_protocols=None):
        c = ks.start_cluster(n_brokers, broker_overrides=broker_overrides,
                             extra_security_protocols=extra_security_protocols)
        created.append(c)
        return c

    try:
        yield _make
    finally:
        for c in created:
            c.shutdown()


@pytest.fixture
def check_avro_compatibility():
    """Return the no-Docker Avro compatibility checker (KFK-06 graded path).

    ``check(reader_schema, writer_schema, mode)`` -> bool, where mode is
    BACKWARD / FORWARD / FULL / NONE. Built on fastavro schema resolution — no
    Schema Registry process needed, so KFK-06 is always gradable on a bare
    machine. Example::

        def test_add_optional_is_backward(check_avro_compatibility):
            assert check_avro_compatibility(new_schema, old_schema, "BACKWARD")
    """
    from de_testkit.avro_compat import check_compatibility
    return check_compatibility


@pytest.fixture(scope="session")
def schema_registry(kafka_broker):
    """A native Confluent Community Schema Registry (vendored by
    ``labs/fetch_schema_registry.sh``) bound to the session ``kafka_broker``.

    Returns an object with ``.url`` and ``.client()`` (a confluent_kafka
    ``SchemaRegistryClient``). SKIPS cleanly when the native SR is NOT vendored
    — in that case use the ``check_avro_compatibility`` fixture (the graded
    path) and treat a live registry as an optional extra layer.
    """
    from de_testkit import avro_compat

    ok, reason = avro_compat.native_sr_available()
    if not ok:
        pytest.skip(reason)
    sr = avro_compat.start_schema_registry(kafka_broker.bootstrap_servers)
    try:
        yield sr
    finally:
        sr.stop()


# ===========================================================================
# Java/JUnit fixtures (vendored JDK 21 + labs/.jars/java)
# ===========================================================================
@pytest.fixture
def java_toolchain():
    """A ``JavaToolchain`` (javac/java paths + the pinned classpath). SKIPS
    cleanly when the JDK or the Java dep jars are missing (run
    ``bash labs/setup_jdk.sh`` / ``bash labs/fetch_java_deps.sh``)."""
    from de_testkit.java_toolkit import resolve_toolchain
    return resolve_toolchain()


@pytest.fixture
def run_junit():
    """Return the ``run_junit`` helper: compile the learner's Java (from
    ``DE_SUBMISSION_DIR``) + the given JUnit 5/AssertJ test sources and run them
    with the JUnit Platform console launcher, surfacing failures in pytest.

        def test_jvm(run_junit, submission_dir):
            run_junit(submission_dir, [TESTS / "OrderAggregatorTest.java"])

    SKIPS cleanly when the JDK/jars are missing. Use ``assert_success=False`` to
    inspect the ``JUnitResult`` instead of failing (e.g. to assert G1 for the
    starter).
    """
    from de_testkit.java_toolkit import run_junit as _run
    return _run


@pytest.fixture
def submission_dir():
    """The learner's code directory (``DE_SUBMISSION_DIR``), as a ``Path``.

    For Java exercises the test does not import a Python module; it passes this
    directory to ``run_junit`` so all ``.java`` under it get compiled.
    """
    import os
    from pathlib import Path

    d = os.environ.get("DE_SUBMISSION_DIR")
    if not d:
        pytest.skip("DE_SUBMISSION_DIR not set (run via `de test`/`de validate`).")
    return Path(d)


# ===========================================================================
# `streaming` lab-guarded fixtures (SKIP when the lab is down)
# ===========================================================================
def _lab_env_ports():
    """Host ports for the streaming lab (overridable; defaults match
    labs/.env / compose.yaml). NEVER 5432/8888 (your own containers)."""
    return {
        "kafka": int(os.environ.get("DE_LAB_KAFKA_PORT", "19092")),
        "schema_registry": int(os.environ.get("DE_LAB_SR_PORT", "8081")),
        "connect": int(os.environ.get("DE_LAB_CONNECT_PORT", "8083")),
        "postgres": int(os.environ.get("DE_LAB_PG_PORT", "5433")),
    }


@pytest.fixture(scope="session")
def lab_kafka():
    """Bootstrap servers for the `streaming` lab's Kafka (host-mapped). SKIPS
    cleanly when the lab is down. Start it with ``de lab up streaming``."""
    host = os.environ.get("DE_LAB_KAFKA_HOST", "localhost")
    port = _lab_env_ports()["kafka"]
    if not _tcp_open(host, port):
        pytest.skip(f"streaming lab Kafka not reachable at {host}:{port}. "
                    "Start it with `de lab up streaming` (Docker). Do NOT run "
                    "`de lab down` — other sessions may be using it.")
    return f"{host}:{port}"


@pytest.fixture(scope="session")
def lab_schema_registry():
    """URL of the `streaming` lab Schema Registry. SKIPS when the lab is down."""
    host = os.environ.get("DE_LAB_SR_HOST", "localhost")
    port = _lab_env_ports()["schema_registry"]
    if not _tcp_open(host, port):
        pytest.skip(f"streaming lab Schema Registry not reachable at "
                    f"{host}:{port}. Start it with `de lab up streaming`.")
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def lab_connect():
    """URL of the `streaming` lab Kafka Connect REST API. SKIPS when down."""
    host = os.environ.get("DE_LAB_CONNECT_HOST", "localhost")
    port = _lab_env_ports()["connect"]
    if not _tcp_open(host, port):
        pytest.skip(f"streaming lab Kafka Connect not reachable at "
                    f"{host}:{port}. Start it with `de lab up streaming`.")
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def lab_postgres():
    """A psycopg/DBAPI-style dict for the `streaming` lab CDC Postgres (host
    port 5433, NEVER 5432). SKIPS when the lab is down or no driver installed.

    Returns ``{"host","port","dbname","user","password"}``; the test opens its
    own connection (psycopg2/psycopg if the exercise needs it)."""
    host = os.environ.get("DE_LAB_PG_HOST", "localhost")
    port = _lab_env_ports()["postgres"]
    if port == 5432:
        pytest.skip("refusing to use host port 5432 (a local Postgres).")
    if not _tcp_open(host, port):
        pytest.skip(f"streaming lab Postgres not reachable at {host}:{port}. "
                    "Start it with `de lab up streaming`.")
    return {
        "host": host, "port": port,
        "dbname": os.environ.get("DE_LAB_PG_DB", "cdc"),
        "user": os.environ.get("DE_LAB_PG_USER", "cdc"),
        "password": os.environ.get("DE_LAB_PG_PASSWORD", "cdc"),
    }


# ===========================================================================
# Flink fixtures (native dist, no Docker; vendored Flink 2.x + JDK 21)
# ===========================================================================
@pytest.fixture
def flink_java():
    """Return ``run_flink_java``: compile the learner's Java (from
    ``DE_SUBMISSION_DIR``) + a generated driver and run it in-process on an
    embedded Flink MiniCluster. The driver prints one ``DE_RESULT=<json>`` line.

        def test_pipeline(flink_java, submission_dir):
            driver = '''
            public class DeDriver {
              public static void main(String[] a) throws Exception {
                var out = my.Pipe.run(java.util.List.of(1,2,3));
                System.out.println("DE_RESULT=" + out.toString());
              }
            }'''
            r = flink_java(submission_dir, driver)
            assert r.ok and r.result == [10,20,30]

    SKIPs cleanly when the Flink dist or JDK is missing (G1 caveat)."""
    from de_testkit.flink_support import run_flink_java
    return run_flink_java


@pytest.fixture(scope="session")
def flink_sql():
    """A session-scoped ``FlinkSqlRunner`` (embedded TableEnvironment). Its
    ``.run(ddl=, query=, mode=, inserts=, limit=)`` returns changelog rows as
    ``[kind, f0, f1, ...]`` where ``kind`` is the RowKind short string
    (``+I``/``-U``/``+U``/``-D``) — for FLK-04 changelog assertions.

        def test_changelog(flink_sql):
            rows = flink_sql.run(
              ddl=["CREATE TABLE src (k STRING, v INT) WITH "
                   "('connector'='values','data-id'='d1')"],
              query="SELECT k, COUNT(*) FROM src GROUP BY k")
            assert [r[0] for r in rows]  # +I / -U / +U ...

    SKIPs cleanly when the Flink dist or JDK is missing."""
    from de_testkit.flink_support import build_sql_runner
    return build_sql_runner()


@pytest.fixture
def flink_cluster():
    """Function-scoped FACTORY for a native local Flink cluster (JobManager +
    N TaskManagers) with REST + fault-injection helpers (FLK-01/06/11/12). Every
    cluster you create is torn down at the end of the test::

        def test_recovery(flink_cluster):
            c = flink_cluster(n_taskmanagers=2)
            job = c.submit_jar(my_jar, entry_class="com.acme.Job")
            c.wait_job_state(job, "RUNNING")
            c.kill_taskmanager()            # SIGKILL a TM
            c.wait_taskmanagers(1)          # the other TM is still there
            c.start_taskmanager()           # add one back -> job recovers
            sp = c.cancel_with_savepoint(job, tmp_path / "sp")

    Signature: ``flink_cluster(n_taskmanagers=1, slots_per_tm=2)``. Ports are
    OS-assigned (parallel-safe; never 5432/8888). SKIPs when the dist/JDK are
    missing."""
    from de_testkit import flink_support as fs

    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    created = []

    def _make(n_taskmanagers: int = 1, *, slots_per_tm: int = 2):
        c = fs.start_local_cluster(n_taskmanagers, slots_per_tm=slots_per_tm)
        created.append(c)
        return c

    try:
        yield _make
    finally:
        for c in created:
            c.shutdown()


@pytest.fixture
def pyflink_runner():
    """Return ``run_pyflink``: run a PyFlink script in the isolated venv
    (``labs/.flink/.pyflink-venv``) on the vendored JDK 21. The script prints one
    ``DE_RESULT=<json>`` line. SKIPs cleanly when the venv/JDK are missing
    (FLK-08 Python side).

        def test_pyflink(pyflink_runner, submission_dir):
            r = pyflink_runner(submission_dir / "job.py")
            assert r.ok and r.result == [...]
    """
    from de_testkit.flink_support import run_pyflink
    return run_pyflink


@pytest.fixture(scope="session")
def lab_flink():
    """REST URL of the `flink` Docker lab's JobManager (host 8086). SKIPS cleanly
    when the lab is down. Start it with ``de lab up flink``. Extra layer only —
    every graded FLK exercise also has a no-Docker (embedded / native-cluster)
    graded part."""
    host = os.environ.get("DE_LAB_FLINK_HOST", "localhost")
    port = int(os.environ.get("DE_LAB_FLINK_REST_PORT", "8086"))
    if port in (5432, 8888):
        pytest.skip("refusing host port 5432/8888 (your own containers).")
    if not _tcp_open(host, port):
        pytest.skip(f"flink lab JobManager REST not reachable at {host}:{port}. "
                    "Start it with `de lab up flink` (Docker). Do NOT run "
                    "`de lab down` — other sessions may be using it.")
    return f"http://{host}:{port}"


# ===========================================================================
# streamhouse fixtures (native, no Docker; Paimon/Fluss/Iceberg)
# ===========================================================================
@pytest.fixture
def paimon_sql(tmp_path):
    """A ``PaimonSqlRunner`` on an isolated tmp Paimon warehouse (no Docker).

    ``.batch(ddl=, inserts=, query=, limit=)`` -> changelog rows (bounded,
    deterministic) and ``.streaming(ddl=, inserts=, query=, *, deadline_s=20)``
    -> rows from a bounded scan with a wall-clock deadline that force-cancels the
    job (never hangs). Each row is ``[kind, f0, f1, ...]``. ``.warehouse`` is the
    tmp warehouse Path (feed it to ``serving_query.paimon``).

        def test_partial_update(paimon_sql):
            rows = paimon_sql.batch(
                ddl=["CREATE TABLE cust (id INT,name STRING,email STRING,"
                     "PRIMARY KEY(id) NOT ENFORCED) WITH "
                     "('merge-engine'='partial-update','bucket'='1')"],
                inserts=["INSERT INTO cust VALUES (1,'Alice',CAST(NULL AS STRING))",
                         "INSERT INTO cust VALUES (1,CAST(NULL AS STRING),'a@x')"],
                query="SELECT * FROM cust")
            assert rows == [["+I", "1", "Alice", "a@x"]]

    SKIPs cleanly when the Flink dist / JDK / Paimon jars are missing
    (``bash labs/fetch_streamhouse.sh``)."""
    from de_testkit.streamhouse_support import build_paimon_runner
    return build_paimon_runner(tmp_path / "paimon_wh")


@pytest.fixture
def iceberg_sql(tmp_path):
    """An ``IcebergSqlRunner`` on an isolated tmp hadoop Iceberg warehouse (no
    Docker). ``.batch(...)`` / ``.streaming(..., deadline_s=30)`` mirror
    ``paimon_sql``; a streaming sink commits on checkpoint and a BOUNDED source
    (filesystem / debezium-json) makes the job finish and commit. ``.warehouse``
    is the tmp warehouse Path (feed it to ``serving_query.iceberg``).

    SKIPs cleanly when the Flink dist / JDK / Iceberg runtime are missing
    (``bash labs/fetch_flink.sh``)."""
    from de_testkit.streamhouse_support import build_iceberg_runner
    return build_iceberg_runner(tmp_path / "iceberg_wh")


@pytest.fixture
def fluss_cluster():
    """Function-scoped FACTORY for a native Fluss local cluster (ZK +
    coordinator + 1 tablet server) on OS-assigned free ports. Every cluster is
    torn down (``local-cluster.sh stop`` + ``pkill``) at test end::

        def test_fluss(fluss_cluster):
            c = fluss_cluster()                         # or fluss_cluster(paimon_warehouse=wh)
            # Flink SQL `fluss` catalog at c.bootstrap; write upsert, read batch

    Signature: ``fluss_cluster(paimon_warehouse=None)`` — pass a warehouse Path
    to enable Fluss->Paimon lake tiering before start. SKIPs cleanly when
    Fluss / Flink / JDK are missing (``bash labs/fetch_streamhouse.sh``)."""
    from de_testkit import streamhouse_support as sh

    ok, reason = sh.fluss_available()
    if not ok:
        pytest.skip(reason)
    created = []

    def _make(*, paimon_warehouse=None):
        c = sh.start_fluss_cluster(paimon_warehouse=paimon_warehouse)
        created.append(c)
        return c

    try:
        yield _make
    finally:
        for c in created:
            c.shutdown()


@pytest.fixture
def serving_query():
    """A ``ServingQuery`` for pure-Python serving reads (no Docker).

    ``.paimon(warehouse, table) -> list[dict]`` reads a Paimon table (merge
    applied) via the isolated pypaimon venv; ``.iceberg(warehouse, db, table) ->
    pyarrow.Table`` reads Iceberg via pyiceberg (register it in DuckDB for SQL).

        def test_serving(paimon_sql, serving_query):
            paimon_sql.batch(ddl=[...], inserts=[...])      # build the table
            rows = serving_query.paimon(str(paimon_sql.warehouse), "default.cust")

    The Paimon side SKIPs cleanly when the pypaimon venv is missing
    (``bash labs/fetch_streamhouse.sh``)."""
    from de_testkit.streamhouse_support import ServingQuery
    return ServingQuery()


@pytest.fixture
def sth_oracle():
    """Return ``compare_streaming_vs_batch(streaming, batch, *,
    drop_rowkind=True)``: order-insensitive assertion that the streaming result
    equals an independent batch rebuild (TESTING.md G6 streaming invariant), with
    a clear multiset diff on failure. Returns ``True`` on agreement.

        def test_invariant(paimon_sql, duckdb_con, sth_oracle):
            streamed = paimon_sql.batch(ddl=[...], inserts=[...], query="SELECT ...")
            oracle = duckdb_con.execute("SELECT ...").fetchall()
            sth_oracle(streamed, oracle)
    """
    from de_testkit.streamhouse_support import compare_streaming_vs_batch
    return compare_streaming_vs_batch


@pytest.fixture(scope="session")
def lab_streamhouse():
    """REST URL of the optional `streamhouse` Docker lab's serving engine (host
    port, NEVER 5432/8888). SKIPS cleanly when the lab is down. Extra layer only
    — every graded STH exercise also runs with NO Docker on the native path
    (``paimon_sql`` / ``iceberg_sql`` / ``fluss_cluster`` / ``serving_query``)."""
    host = os.environ.get("DE_LAB_STREAMHOUSE_HOST", "localhost")
    port = int(os.environ.get("DE_LAB_STREAMHOUSE_PORT", "8089"))
    if port in (5432, 8888):
        pytest.skip("refusing host port 5432/8888 (your own containers).")
    if not _tcp_open(host, port):
        pytest.skip(f"streamhouse lab not reachable at {host}:{port}. Start it "
                    "with `de lab up streamhouse` (Docker). Do NOT run `de lab "
                    "down` — other sessions may be using it.")
    return f"http://{host}:{port}"
