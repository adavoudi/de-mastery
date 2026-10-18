"""Native streamhouse support for the de_testkit fixtures (STH track).

NO DOCKER. Everything here reuses the vendored Apache Flink 2.2.1 dist
(``labs/.flink/flink-2.2.1``), the vendored Temurin JDK 21 (``labs/.jdk``), the
native Apache Fluss 1.0.0 server (``labs/.fluss/fluss-1.0.0``) and the Paimon /
Fluss / Iceberg jars in ``labs/.jars/streamhouse`` + ``labs/.jars/flink``, all
fetched + checksum-verified by ``labs/fetch_streamhouse.sh``. This is what makes
every graded STH exercise satisfy the no-Docker rule.

Surfaces (all probed and verified 2026-10-04):

* **Paimon Flink SQL** (``paimon_sql`` -> ``PaimonSqlRunner``). Runs Flink SQL on
  an embedded ``TableEnvironment`` against an isolated tmp Paimon warehouse. Two
  helpers: ``batch(...)`` (``inBatchMode`` — bounded, deterministic, the common
  path for merge/changelog/partial-update reads) and ``streaming(..., deadline_s)``
  (``inStreamingMode`` with a bounded scan + a wall-clock deadline that CANCELS
  the job via ``TableResult.getJobClient().cancel()`` — never iterates forever).

* **Fluss local cluster** (``fluss_cluster`` -> ``FlussCluster``). Starts ZK +
  coordinator + 1 tablet server from the dist on OS-assigned free ports with an
  isolated ``FLUSS_CONF_DIR`` / data dirs, and tears them ALL down in a trap
  (``local-cluster.sh stop`` + ``pkill`` of the per-run work dir). Optional lake
  tiering to Paimon via ``enable_paimon_tiering(warehouse)`` before start.

* **Iceberg Flink catalog** (``iceberg_sql`` -> ``IcebergSqlRunner``). Runs Flink
  SQL on an embedded ``TableEnvironment`` with a local hadoop Iceberg catalog
  rooted in a tmp warehouse. Sink commits on checkpoint; reads are batch.

* **Serving** (``serving_query`` -> ``ServingQuery``). Pure-Python, no Docker:
  ``paimon(warehouse, table)`` reads a Paimon table via the isolated pypaimon
  venv (``labs/.streamhouse/.pypaimon-venv``); ``iceberg(warehouse, db, table)``
  reads Iceberg via pyiceberg -> Arrow and lets you run DuckDB SQL over it.

* **Oracle** (``sth_oracle`` -> ``compare_streaming_vs_batch``). Order-insensitive
  comparison of a streaming result against an independent batch rebuild, with a
  clear per-row diff (the TESTING.md G6 streaming invariant).

Every entry point raises ``pytest.skip`` (never fail) when its vendored runtime
is missing, with the exact fetch command — the G1 caveat from earlier phases.
Every helper that waits has a deadline; nothing iterates an unbounded stream.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from de_testkit.java_env import repo_root
from de_testkit.java_env import resolve as resolve_java
from de_testkit.flink_support import (
    JAVA17_OPENS,
    flink_home,
    flink_jars_dir,
    free_port,
)

FETCH_CMD = "bash labs/fetch_streamhouse.sh"
PAIMON_JAR = "paimon-flink-2.2-2.0.0.jar"
FLUSS_CONNECTOR = "fluss-flink-2.2-1.0.0.jar"
FLUSS_TIERING = "fluss-flink-tiering-1.0.0.jar"
ICEBERG_JAR = "iceberg-flink-runtime-2.2-1.12.0.jar"
HADOOP_JARS = ("hadoop-client-api-3.4.2.jar", "hadoop-client-runtime-3.4.2.jar")


# ---------------------------------------------------------------------------
# artifact discovery + availability
# ---------------------------------------------------------------------------
def streamhouse_jars_dir(root: Path | None = None) -> Path:
    return (root or repo_root()) / "labs" / ".jars" / "streamhouse"


def fluss_home(root: Path | None = None) -> Path | None:
    """Return ``labs/.fluss/fluss-<ver>`` if it has ``bin/local-cluster.sh``."""
    base = (root or repo_root()) / "labs" / ".fluss"
    if not base.is_dir():
        return None
    for child in sorted(base.iterdir()):
        if (child / "bin" / "local-cluster.sh").exists():
            return child
    return None


def pypaimon_venv(root: Path | None = None) -> Path | None:
    """Return the isolated pypaimon venv python if present."""
    py = ((root or repo_root()) / "labs" / ".streamhouse" / ".pypaimon-venv"
          / "bin" / "python")
    return py if py.exists() else None


def _java_home() -> str:
    return resolve_java()["java_home"] or ""


def _env(extra: dict | None = None) -> dict:
    env = dict(os.environ)
    jh = _java_home()
    if jh:
        env["JAVA_HOME"] = jh
        env["PATH"] = str(Path(jh) / "bin") + os.pathsep + env.get("PATH", "")
    if extra:
        env.update(extra)
    return env


def _have(path: Path) -> bool:
    return path.exists()


def paimon_available(root: Path | None = None) -> tuple[bool, str]:
    """(ok, reason). ok iff the Flink dist, JDK, Paimon + hadoop jars exist."""
    if flink_home(root) is None:
        return False, ("native Flink not found. Run `bash labs/fetch_flink.sh`, "
                       f"then `{FETCH_CMD}` for the Paimon jars.")
    if not resolve_java()["usable"]:
        return False, ("Java unusable. Run `bash labs/setup_jdk.sh` to vendor "
                       "JDK 21.")
    jd = streamhouse_jars_dir(root)
    fj = flink_jars_dir(root)
    if not _have(jd / PAIMON_JAR):
        return False, (f"Paimon jar missing ({PAIMON_JAR}). Run `{FETCH_CMD}`.")
    for h in HADOOP_JARS:
        if not _have(fj / h):
            return False, (f"hadoop jar missing ({h}). Run `bash labs/fetch_flink.sh`.")
    return True, ""


def iceberg_available(root: Path | None = None) -> tuple[bool, str]:
    if flink_home(root) is None:
        return False, "native Flink not found. Run `bash labs/fetch_flink.sh`."
    if not resolve_java()["usable"]:
        return False, "Java unusable. Run `bash labs/setup_jdk.sh`."
    fj = flink_jars_dir(root)
    if not _have(fj / ICEBERG_JAR):
        return False, (f"Iceberg Flink runtime missing ({ICEBERG_JAR}). "
                       "Run `bash labs/fetch_flink.sh`.")
    for h in HADOOP_JARS:
        if not _have(fj / h):
            return False, (f"hadoop jar missing ({h}). Run `bash labs/fetch_flink.sh`.")
    return True, ""


def fluss_available(root: Path | None = None) -> tuple[bool, str]:
    if fluss_home(root) is None:
        return False, (f"native Fluss not found. Run `{FETCH_CMD}` to vendor the "
                       "Fluss 1.0.0 server into labs/.fluss/.")
    if flink_home(root) is None:
        return False, "native Flink not found. Run `bash labs/fetch_flink.sh`."
    if not resolve_java()["usable"]:
        return False, "Java unusable. Run `bash labs/setup_jdk.sh`."
    if not _have(streamhouse_jars_dir(root) / FLUSS_CONNECTOR):
        return False, (f"Fluss Flink connector missing ({FLUSS_CONNECTOR}). "
                       f"Run `{FETCH_CMD}`.")
    return True, ""


def serving_available(root: Path | None = None) -> tuple[bool, str]:
    if pypaimon_venv(root) is None:
        return False, ("pypaimon venv not found (labs/.streamhouse/.pypaimon-venv)."
                       f" Run `{FETCH_CMD}` which builds it; STH-07 Paimon serving "
                       "SKIPs until present.")
    return True, ""


def _paimon_classpath(root: Path | None = None) -> str:
    home = flink_home(root)
    cp = [str(p) for p in sorted((home / "lib").glob("*.jar"))]
    jd = streamhouse_jars_dir(root)
    fj = flink_jars_dir(root)
    cp.append(str(jd / PAIMON_JAR))
    for h in HADOOP_JARS:
        cp.append(str(fj / h))
    return os.pathsep.join(cp)


def _iceberg_classpath(root: Path | None = None) -> str:
    home = flink_home(root)
    cp = [str(p) for p in sorted((home / "lib").glob("*.jar"))]
    fj = flink_jars_dir(root)
    cp.append(str(fj / ICEBERG_JAR))
    for h in HADOOP_JARS:
        cp.append(str(fj / h))
    return os.pathsep.join(cp)


# ---------------------------------------------------------------------------
# Embedded Flink-SQL harness (shared by Paimon + Iceberg runners)
# ---------------------------------------------------------------------------
# Reads a JSON spec file (argv[0]):
#   {"mode":"batch"|"streaming", "catalog":[ddl...], "ddl":[ddl...],
#    "inserts":[stmt...], "query":"SELECT ...", "limit":N, "deadline_s":D}
# Runs catalog+ddl, awaits each insert, then collects the query. In STREAMING
# mode a background watchdog cancels the job via getJobClient().cancel() once
# `deadline_s` elapses OR `limit` rows are read — so a bounded result returns
# promptly and an unbounded one is force-cancelled (never hangs). Prints one
# line: DE_RESULT=[["+I","f0","f1"], ...] (RowKind short string + fields).
_SQL_HARNESS = r"""
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.core.execution.JobClient;
import java.nio.file.*;
import java.util.*;

public class DeShHarness {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        String raw = new String(Files.readAllBytes(Paths.get(args[0])));
        Map<String,Object> spec = (Map<String,Object>) new Json(raw).value();
        String mode = (String) spec.getOrDefault("mode", "batch");
        EnvironmentSettings s = "streaming".equals(mode)
            ? EnvironmentSettings.inStreamingMode() : EnvironmentSettings.inBatchMode();
        TableEnvironment t = TableEnvironment.create(s);
        for (Object d : (List<Object>) spec.getOrDefault("catalog", new ArrayList<>()))
            t.executeSql((String) d);
        for (Object d : (List<Object>) spec.getOrDefault("ddl", new ArrayList<>()))
            t.executeSql((String) d);
        for (Object ins : (List<Object>) spec.getOrDefault("inserts", new ArrayList<>()))
            t.executeSql((String) ins).await();
        String query = (String) spec.get("query");
        if (query == null) { System.out.println("DE_RESULT=[]"); return; }
        int limit = ((Number) spec.getOrDefault("limit", 1000)).intValue();
        double deadlineS = ((Number) spec.getOrDefault("deadline_s", 60)).doubleValue();
        TableResult r = t.executeSql(query);
        final JobClient[] jc = new JobClient[]{ r.getJobClient().orElse(null) };
        final long deadlineMs = System.currentTimeMillis() + (long)(deadlineS * 1000);
        // watchdog: force-cancel the job if we blow the wall-clock deadline.
        Thread wd = new Thread(() -> {
            while (System.currentTimeMillis() < deadlineMs) {
                try { Thread.sleep(100); } catch (InterruptedException e) { return; }
            }
            if (jc[0] != null) { try { jc[0].cancel(); } catch (Throwable ig) {} }
        });
        wd.setDaemon(true); wd.start();
        List<List<String>> out = new ArrayList<>();
        try (CloseableIterator<Row> it = r.collect()) {
            int n = 0;
            while (n < limit && System.currentTimeMillis() < deadlineMs && it.hasNext()) {
                Row row = it.next();
                List<String> rec = new ArrayList<>();
                rec.add(row.getKind().shortString());
                for (int i = 0; i < row.getArity(); i++) {
                    Object f = row.getField(i);
                    rec.add(f == null ? null : String.valueOf(f));
                }
                out.add(rec); n++;
            }
        } catch (Throwable ig) { /* cancel may surface as an iterator error */ }
        wd.interrupt();
        if (jc[0] != null) { try { jc[0].cancel(); } catch (Throwable ig) {} }
        System.out.println("DE_RESULT=" + toJson(out));
    }

    static String toJson(Object o) { StringBuilder b=new StringBuilder(); w(b,o); return b.toString(); }
    static void w(StringBuilder b, Object o) {
        if (o==null){b.append("null");return;}
        if (o instanceof List){b.append('[');List<?> l=(List<?>)o;
            for(int i=0;i<l.size();i++){if(i>0)b.append(',');w(b,l.get(i));}b.append(']');return;}
        if (o instanceof String){b.append('"');
            for(char c:((String)o).toCharArray()){
                if(c=='"'||c=='\\')b.append('\\').append(c);
                else if(c=='\n')b.append("\\n");else if(c=='\r')b.append("\\r");
                else if(c=='\t')b.append("\\t");else b.append(c);} b.append('"');return;}
        b.append(String.valueOf(o));
    }
    static class Json {
        final String s; int i; Json(String s){this.s=s;}
        Object value(){ ws(); char c=s.charAt(i);
            if(c=='{')return obj(); if(c=='[')return arr(); if(c=='"')return str();
            if(c=='t'||c=='f')return bool(); if(c=='n'){i+=4;return null;} return num(); }
        void ws(){ while(i<s.length()&&Character.isWhitespace(s.charAt(i)))i++; }
        Map<String,Object> obj(){ Map<String,Object> m=new LinkedHashMap<>(); i++; ws();
            if(s.charAt(i)=='}'){i++;return m;}
            while(true){ ws(); String k=str(); ws(); i++; Object v=value(); m.put(k,v); ws();
                if(s.charAt(i)==','){i++;continue;} i++; break;} return m; }
        List<Object> arr(){ List<Object> l=new ArrayList<>(); i++; ws();
            if(s.charAt(i)==']'){i++;return l;}
            while(true){ Object v=value(); l.add(v); ws();
                if(s.charAt(i)==','){i++;continue;} i++; break;} return l; }
        String str(){ ws(); StringBuilder b=new StringBuilder(); i++;
            while(s.charAt(i)!='"'){ char c=s.charAt(i++);
                if(c=='\\'){ char n=s.charAt(i++); switch(n){
                    case 'n':b.append('\n');break;case 't':b.append('\t');break;
                    case 'r':b.append('\r');break;case '"':b.append('"');break;
                    case '\\':b.append('\\');break;case '/':b.append('/');break;default:b.append(n);} }
                else b.append(c);} i++; return b.toString(); }
        Object bool(){ if(s.charAt(i)=='t'){i+=4;return Boolean.TRUE;} i+=5; return Boolean.FALSE; }
        Object num(){ int st=i; while(i<s.length()&&"-+.eE0123456789".indexOf(s.charAt(i))>=0)i++;
            String t=s.substring(st,i);
            if(t.contains(".")||t.contains("e")||t.contains("E")) return Double.parseDouble(t);
            return Integer.parseInt(t); }
    }
}
"""


def _compile_harness(cp: str) -> str:
    """Compile DeShHarness once; return the classes dir."""
    jh = _java_home()
    base = Path(tempfile.mkdtemp(prefix="sth-harness-"))
    src = base / "DeShHarness.java"
    src.write_text(_SQL_HARNESS)
    classes = base / "classes"
    classes.mkdir()
    comp = subprocess.run(
        [str(Path(jh) / "bin" / "javac"), "-proc:none", "-cp", cp,
         "-d", str(classes), str(src)],
        capture_output=True, text=True, timeout=180, env=_env())
    if comp.returncode != 0:  # pragma: no cover - static harness
        raise RuntimeError("streamhouse SQL harness failed to compile:\n"
                           + comp.stderr)
    return str(classes)


def _run_harness(cp: str, classes: str, spec: dict, *, timeout_s: int) -> list:
    jh = _java_home()
    work = Path(tempfile.mkdtemp(prefix="sth-sql-run-"))
    try:
        spec_path = work / "spec.json"
        spec_path.write_text(json.dumps(spec))
        r = subprocess.run(
            [str(Path(jh) / "bin" / "java"), *JAVA17_OPENS, "-Xmx1500m",
             "-cp", cp + os.pathsep + classes, "DeShHarness", str(spec_path)],
            capture_output=True, text=True, timeout=timeout_s, env=_env())
        if r.returncode != 0:
            raise RuntimeError("streamhouse SQL harness failed:\nSTDOUT:\n"
                               + r.stdout + "\nSTDERR:\n" + r.stderr[-3000:])
        for line in r.stdout.splitlines():
            if line.startswith("DE_RESULT="):
                return json.loads(line[len("DE_RESULT="):])
        raise RuntimeError("no DE_RESULT from harness:\n" + r.stdout
                           + "\n" + r.stderr[-2000:])
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# PaimonSqlRunner
# ---------------------------------------------------------------------------
@dataclass
class PaimonSqlRunner:
    """Run Flink SQL against an isolated tmp Paimon warehouse (no Docker).

    A ``paimon`` catalog named ``pc`` on a ``file://`` warehouse under ``tmp`` is
    pre-created and ``USE``d for every call. Pass table DDL + inserts + a query.

    ``batch(ddl, inserts, query, limit=1000, timeout_s=180)`` -> rows (bounded,
    deterministic — the common path for merge/partial-update/changelog reads).

    ``streaming(ddl, inserts, query, *, deadline_s=20, limit=1000,
    timeout_s=120)`` -> rows read from a bounded scan with a wall-clock deadline;
    the job is force-cancelled when the deadline or ``limit`` is hit, so it NEVER
    hangs on an unbounded stream.

    Each row is ``[kind, f0, f1, ...]`` (RowKind short string + string fields).
    Example (STH-01 partial-update)::

        rows = paimon_sql.batch(
            ddl=["CREATE TABLE cust (id INT,name STRING,email STRING,"
                 "PRIMARY KEY(id) NOT ENFORCED) WITH "
                 "('merge-engine'='partial-update','bucket'='1')"],
            inserts=["INSERT INTO cust VALUES (1,'Alice',CAST(NULL AS STRING))",
                     "INSERT INTO cust VALUES (1,CAST(NULL AS STRING),'a@x.com')"],
            query="SELECT * FROM cust")
        assert rows == [["+I", "1", "Alice", "a@x.com"]]
    """
    warehouse: Path
    _cp: str
    _classes: str

    def _catalog_ddl(self) -> list[str]:
        return [
            f"CREATE CATALOG pc WITH ('type'='paimon','warehouse'='file://{self.warehouse}')",
            "USE CATALOG pc",
        ]

    def batch(self, ddl=None, inserts=None, query=None, *, limit: int = 1000,
              timeout_s: int = 180):
        spec = {"mode": "batch", "catalog": self._catalog_ddl(),
                "ddl": list(ddl or []), "inserts": list(inserts or []),
                "query": query, "limit": limit, "deadline_s": timeout_s - 10}
        return _run_harness(self._cp, self._classes, spec, timeout_s=timeout_s)

    def streaming(self, ddl=None, inserts=None, query=None, *,
                  deadline_s: float = 20.0, limit: int = 1000,
                  timeout_s: int = 120):
        spec = {"mode": "streaming", "catalog": self._catalog_ddl(),
                "ddl": list(ddl or []), "inserts": list(inserts or []),
                "query": query, "limit": limit, "deadline_s": deadline_s}
        return _run_harness(self._cp, self._classes, spec, timeout_s=timeout_s)


def build_paimon_runner(tmp_warehouse: Path, root: Path | None = None) -> PaimonSqlRunner:
    """Compile the harness and return a PaimonSqlRunner on ``tmp_warehouse``.
    SKIPs cleanly when the dist/JDK/Paimon jars are missing."""
    import pytest

    ok, reason = paimon_available(root)
    if not ok:
        pytest.skip(reason)
    tmp_warehouse = Path(tmp_warehouse)
    tmp_warehouse.mkdir(parents=True, exist_ok=True)
    cp = _paimon_classpath(root)
    classes = _compile_harness(cp)
    return PaimonSqlRunner(warehouse=tmp_warehouse, _cp=cp, _classes=classes)


# ---------------------------------------------------------------------------
# IcebergSqlRunner
# ---------------------------------------------------------------------------
@dataclass
class IcebergSqlRunner:
    """Run Flink SQL against a local hadoop Iceberg catalog (no Docker).

    A catalog ``ice`` (``catalog-type=hadoop``, ``file://`` warehouse under
    ``tmp``) is pre-created. A streaming sink commits Iceberg snapshots on
    checkpoint; a **bounded source** (filesystem / debezium-json) makes the job
    finish and fire the final commit — then read back in batch.

    ``batch(ddl, inserts, query, ...)`` and
    ``streaming(ddl, inserts, query, *, deadline_s=30, ...)`` mirror
    ``PaimonSqlRunner``. Example (STH-04 Iceberg-as-streaming-storage)::

        ice.streaming(
            ddl=["CREATE TABLE ice.db.c (id INT,amt INT,PRIMARY KEY(id) NOT "
                 "ENFORCED) WITH ('format-version'='2')",
                 "CREATE TABLE cdc (id INT,amt INT,PRIMARY KEY(id) NOT ENFORCED)"
                 " WITH ('connector'='filesystem','path'='"+p+"',"
                 "'format'='debezium-json')"],
            inserts=["INSERT INTO ice.db.c SELECT id,amt FROM cdc"], query=None)
        rows = ice.batch(query="SELECT count(*),sum(amt) FROM ice.db.c")
    """
    warehouse: Path
    _cp: str
    _classes: str

    def _catalog_ddl(self) -> list[str]:
        return [
            f"CREATE CATALOG ice WITH ('type'='iceberg','catalog-type'='hadoop',"
            f"'warehouse'='file://{self.warehouse}')",
            "CREATE DATABASE IF NOT EXISTS ice.db",
        ]

    def batch(self, ddl=None, inserts=None, query=None, *, limit: int = 1000,
              timeout_s: int = 180):
        spec = {"mode": "batch", "catalog": self._catalog_ddl(),
                "ddl": list(ddl or []), "inserts": list(inserts or []),
                "query": query, "limit": limit, "deadline_s": timeout_s - 10}
        return _run_harness(self._cp, self._classes, spec, timeout_s=timeout_s)

    def streaming(self, ddl=None, inserts=None, query=None, *,
                  deadline_s: float = 30.0, limit: int = 1000,
                  timeout_s: int = 180):
        spec = {"mode": "streaming", "catalog": self._catalog_ddl(),
                "ddl": list(ddl or []), "inserts": list(inserts or []),
                "query": query, "limit": limit, "deadline_s": deadline_s}
        return _run_harness(self._cp, self._classes, spec, timeout_s=timeout_s)


def build_iceberg_runner(tmp_warehouse: Path, root: Path | None = None) -> IcebergSqlRunner:
    """Compile the harness and return an IcebergSqlRunner on ``tmp_warehouse``.
    SKIPs cleanly when the dist/JDK/Iceberg jars are missing."""
    import pytest

    ok, reason = iceberg_available(root)
    if not ok:
        pytest.skip(reason)
    tmp_warehouse = Path(tmp_warehouse)
    tmp_warehouse.mkdir(parents=True, exist_ok=True)
    cp = _iceberg_classpath(root)
    classes = _compile_harness(cp)
    return IcebergSqlRunner(warehouse=tmp_warehouse, _cp=cp, _classes=classes)


# ---------------------------------------------------------------------------
# FlussCluster — native ZK + coordinator + 1 tablet server, guaranteed teardown
# ---------------------------------------------------------------------------
class FlussCluster:
    """A native Fluss local cluster (ZK + coordinator + 1 tablet server).

    Started from ``labs/.fluss`` on OS-assigned free ports with an isolated
    ``FLUSS_CONF_DIR`` and per-run data dirs under ``tmp``. ALWAYS torn down
    (``local-cluster.sh stop`` + ``pkill -f <work dir>``). Enable Paimon lake
    tiering BEFORE ``start()`` with ``enable_paimon_tiering(warehouse)``.

    Attributes: ``bootstrap`` (``127.0.0.1:<coord>`` for the Flink ``fluss``
    catalog), ``coordinator_port``, ``zk_port``, ``warehouse`` (set after
    tiering is enabled). Example::

        c = fluss_cluster()                 # fixture factory
        c.start()
        # write via Flink SQL fluss catalog at c.bootstrap, read batch, assert
    """

    def __init__(self, home: Path, work: Path):
        self._home = home
        self._work = work
        self.conf_dir = work / "conf"
        self.conf_dir.mkdir(parents=True, exist_ok=True)
        self.zk_port = free_port()
        self.coordinator_port = free_port()
        self.warehouse: Path | None = None
        self._tiering: dict | None = None
        self._started = False

    @property
    def bootstrap(self) -> str:
        return f"127.0.0.1:{self.coordinator_port}"

    def enable_paimon_tiering(self, warehouse: str | Path) -> None:
        if self._started:
            raise RuntimeError("enable tiering before start()")
        self.warehouse = Path(warehouse)
        self.warehouse.mkdir(parents=True, exist_ok=True)
        self._tiering = {
            "datalake.format": "paimon",
            "datalake.paimon.metastore": "filesystem",
            "datalake.paimon.warehouse": f"file://{self.warehouse}",
        }

    def _write_conf(self) -> None:
        data = self._work / "fdata"
        remote = self._work / "fremote"
        zk_data = self._work / "fzk"
        for d in (data, remote, zk_data):
            d.mkdir(parents=True, exist_ok=True)
        lines = [
            f"zookeeper.address: localhost:{self.zk_port}",
            "default.bucket.number: 1",
            "default.replication.factor: 1",
            f"data.dir: {data}",
            f"remote.data.dir: {remote}",
            f"bind.listeners: FLUSS://localhost:{self.coordinator_port}",
            "tablet-server.id: 0",
        ]
        if self._tiering:
            lines += [f"{k}: {v}" for k, v in self._tiering.items()]
        (self.conf_dir / "server.yaml").write_text("\n".join(lines) + "\n")
        (self.conf_dir / "zookeeper.properties").write_text(
            f"dataDir={zk_data}\nclientPort={self.zk_port}\n"
            "maxClientCnxns=0\nadmin.enableServer=false\n")

    def _env(self) -> dict:
        return _env({
            "FLUSS_CONF_DIR": str(self.conf_dir),
            "FLUSS_LOG_DIR": str(self._work / "flog"),
            "FLUSS_PID_DIR": str(self._work / "fpid"),
        })

    def start(self, *, deadline_s: float = 60.0) -> "FlussCluster":
        self._write_conf()
        (self._work / "flog").mkdir(parents=True, exist_ok=True)
        (self._work / "fpid").mkdir(parents=True, exist_ok=True)
        subprocess.run([str(self._home / "bin" / "local-cluster.sh"), "start"],
                       env=self._env(), capture_output=True, text=True,
                       timeout=deadline_s, start_new_session=True)
        self._started = True
        self._wait_coordinator(deadline_s=deadline_s)
        return self

    def _wait_coordinator(self, *, deadline_s: float) -> None:
        deadline = time.time() + deadline_s
        while time.time() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.0)
                try:
                    s.connect(("127.0.0.1", self.coordinator_port))
                    return
                except OSError:
                    time.sleep(0.5)
        raise TimeoutError(
            f"Fluss coordinator not reachable on {self.coordinator_port} "
            f"within {deadline_s}s")

    def shutdown(self) -> None:
        if self._started:
            try:
                subprocess.run(
                    [str(self._home / "bin" / "local-cluster.sh"), "stop"],
                    env=self._env(), capture_output=True, text=True,
                    timeout=30, start_new_session=True)
            except Exception:  # noqa: BLE001
                pass
        # belt + braces: kill anything still referencing our work dir.
        try:
            subprocess.run(["pkill", "-9", "-f", str(self._work)],
                           capture_output=True, timeout=15)
        except Exception:  # noqa: BLE001
            pass
        shutil.rmtree(self._work, ignore_errors=True)


def start_fluss_cluster(root: Path | None = None, *,
                        paimon_warehouse: str | Path | None = None) -> FlussCluster:
    """Start a native Fluss local cluster. SKIPs cleanly when Fluss/Flink/JDK
    are missing. Pass ``paimon_warehouse`` to enable lake tiering to Paimon."""
    import pytest

    ok, reason = fluss_available(root)
    if not ok:
        pytest.skip(reason)
    home = fluss_home(root)
    work = Path(tempfile.mkdtemp(prefix="sth-fluss-"))
    c = FlussCluster(home, work)
    if paimon_warehouse is not None:
        c.enable_paimon_tiering(paimon_warehouse)
    try:
        return c.start()
    except Exception:
        c.shutdown()
        raise


# ---------------------------------------------------------------------------
# ServingQuery — pure-Python serving reads (no Docker)
# ---------------------------------------------------------------------------
# pypaimon runs in its own venv; this tiny script reads a table and prints a
# JSON record batch (list of column-wise dicts -> we emit row dicts).
_PYPAIMON_READER = r"""
import json, sys
from pypaimon import CatalogFactory
wh, table = sys.argv[1], sys.argv[2]
cat = CatalogFactory.create({"warehouse": wh})
t = cat.get_table(table)
rb = t.new_read_builder()
arr = rb.new_read().to_arrow(rb.new_scan().plan().splits())
print("DE_RESULT=" + json.dumps(arr.to_pylist(), default=str))
"""


@dataclass
class ServingQuery:
    """Serving-layer reads over streamhouse tables (pure Python, no Docker).

    ``paimon(warehouse, table) -> list[dict]`` reads a Paimon table (merge
    applied) via the isolated pypaimon venv. ``table`` is ``db.name`` (default
    db is ``default``; a Fluss-tiered table lives under db ``fluss``).

    ``iceberg(warehouse, db, table) -> pyarrow.Table`` reads an Iceberg table via
    pyiceberg; feed it to ``duckdb`` with ``con.register("t", arr)``. Example::

        rows = serving_query.paimon(str(paimon_sql.warehouse), "default.cust")
        arr = serving_query.iceberg(str(ice.warehouse), "db", "customers")
    """
    _root: Path | None = None

    def paimon(self, warehouse: str, table: str, *, timeout_s: int = 120) -> list:
        import pytest

        ok, reason = serving_available(self._root)
        if not ok:
            pytest.skip(reason)
        py = pypaimon_venv(self._root)
        script = Path(tempfile.mkdtemp(prefix="sth-serve-")) / "r.py"
        script.write_text(_PYPAIMON_READER)
        try:
            r = subprocess.run([str(py), str(script), warehouse, table],
                               capture_output=True, text=True, timeout=timeout_s)
            if r.returncode != 0:
                raise RuntimeError("pypaimon read failed:\n" + r.stderr[-3000:])
            for line in r.stdout.splitlines():
                if line.startswith("DE_RESULT="):
                    return json.loads(line[len("DE_RESULT="):])
            raise RuntimeError("no DE_RESULT from pypaimon reader:\n" + r.stdout)
        finally:
            shutil.rmtree(script.parent, ignore_errors=True)

    def iceberg(self, warehouse: str, db: str, table: str):
        import pytest

        try:
            from pyiceberg.table import StaticTable
        except Exception as exc:  # pragma: no cover
            pytest.skip(f"pyiceberg not available: {exc}")
        meta_dir = Path(warehouse) / db / table / "metadata"
        metas = sorted(meta_dir.glob("v*.metadata.json"))
        if not metas:
            raise FileNotFoundError(f"no Iceberg metadata under {meta_dir}")
        return StaticTable.from_metadata(str(metas[-1])).scan().to_arrow()


# ---------------------------------------------------------------------------
# Oracle — streaming result vs batch rebuild (order-insensitive, clear diffs)
# ---------------------------------------------------------------------------
def _normalise_row(row) -> tuple:
    """Normalise a row to a hashable, order-insensitive key.

    Accepts a dict (values sorted by key), a list/tuple (as-is), or a scalar.
    Values are stringified so ``1`` and ``"1"`` compare equal (the SQL harness
    returns string fields; a DuckDB/Python oracle may return ints).
    """
    if isinstance(row, dict):
        return tuple(str(row[k]) for k in sorted(row))
    if isinstance(row, (list, tuple)):
        return tuple(str(x) for x in row)
    return (str(row),)


def compare_streaming_vs_batch(streaming, batch, *, drop_rowkind: bool = True):
    """Assert the streaming result equals the batch rebuild, order-insensitive.

    This is the TESTING.md G6 streaming invariant: once all input has arrived and
    watermarks have passed, the streaming FINAL state must equal the batch answer
    over the same events.

    * ``streaming`` — rows from a ``PaimonSqlRunner.streaming``/``batch`` call,
      i.e. ``[kind, f0, f1, ...]``. With ``drop_rowkind=True`` the leading
      RowKind column is dropped before comparison (final-state semantics).
    * ``batch`` — the independent oracle rows (list of lists/tuples/dicts), e.g.
      from DuckDB over the same seed. RowKind is assumed absent.

    Raises ``AssertionError`` with a clear ``only in streaming`` /
    ``only in batch`` multiset diff. Returns ``True`` on agreement.
    """
    def strip(rows, drop):
        out = []
        for r in rows:
            if drop and isinstance(r, (list, tuple)) and r and str(r[0]) in (
                    "+I", "-U", "+U", "-D"):
                r = list(r)[1:]
            out.append(_normalise_row(r))
        return out

    s = strip(streaming, drop_rowkind)
    b = strip(batch, False)
    from collections import Counter
    cs, cb = Counter(s), Counter(b)
    if cs == cb:
        return True
    only_s = list((cs - cb).elements())
    only_b = list((cb - cs).elements())
    raise AssertionError(
        "streaming result != batch rebuild (order-insensitive):\n"
        f"  only in streaming ({len(only_s)}): {only_s[:20]}\n"
        f"  only in batch     ({len(only_b)}): {only_b[:20]}\n"
        f"  streaming n={len(s)} batch n={len(b)}")
