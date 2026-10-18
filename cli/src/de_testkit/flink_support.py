"""Native Apache Flink support for the de_testkit fixtures (FLK track).

NO DOCKER. Everything here uses the vendored Flink 2.x distribution
(``labs/.flink/flink-<ver>``, from ``labs/fetch_flink.sh``) and the vendored
Temurin JDK 21 (``labs/.jdk``, from ``labs/setup_jdk.sh``). This is what makes
every graded FLK exercise satisfy the no-Docker rule.

Four graded surfaces, all probed + verified on 2026-10-04 (Flink 2.2.1, JDK 21):

* **Embedded Java jobs** (DataStream / Table API). ``run_flink_java`` compiles
  the learner's ``.java`` (from ``DE_SUBMISSION_DIR``) plus a tiny generated
  *driver* against the Flink dist classpath, then runs the driver IN-PROCESS.
  ``StreamExecutionEnvironment.getExecutionEnvironment()`` /
  ``TableEnvironment.create(...)`` spin up a local MiniCluster automatically — no
  ``flink-test-utils`` jar, no cluster process. The driver prints one JSON line
  (``DE_RESULT=...``) which is parsed back into Python. (FLK-02/03/05/06/07/08)

* **Flink SQL / changelog** (``flink_sql`` -> ``FlinkSqlRunner``). Runs SQL on an
  embedded ``TableEnvironment`` through a bundled Java harness and returns the
  **changelog rows with RowKind** (``+I`` / ``-U`` / ``+U`` / ``-D``) — exactly
  what FLK-04 needs. Verified: ``Row.getKind().shortString()`` yields ``+I`` etc.

* **Native local cluster** (``flink_cluster`` -> ``LocalFlinkCluster``). Starts a
  real JobManager + N TaskManagers from the dist on OS-assigned free ports with
  small heaps, exposing REST helpers: submit a jar, list jobs, job plan/vertices,
  parallelism, backpressure, checkpoints, cancel-with-savepoint, and
  **kill a TaskManager process and wait for recovery** (FLK-01/06/11/12).

* **PyFlink** (``pyflink_runner``). apache-flink pulls apache-beam, whose pins
  conflict with the main project (pyarrow/numpy/protobuf), so PyFlink lives in a
  DEDICATED venv (``labs/.flink/.pyflink-venv``) built by ``fetch_flink.sh``. The
  runner shells out to that interpreter with ``JAVA_HOME`` set to the vendored
  JDK and returns the job's JSON result. SKIPs cleanly if the venv is absent
  (FLK-08 Python side).

Every entry point raises ``pytest.skip`` (never fail) when its vendored runtime
is missing, with the exact fetch command — the G1 caveat from earlier phases.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from de_testkit.java_env import repo_root, resolve as resolve_java

HEAP_JM = "1024m"
HEAP_TM = "1280m"
READY_TIMEOUT_S = 90.0

# JVM module opens Flink 2.x needs on Java 17+ (copied from the shipped
# config.yaml env.java.opts.all). Required for embedded jobs AND the native
# cluster JVMs on JDK 21.
JAVA17_OPENS = [
    "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
    "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.text=ALL-UNNAMED",
    "--add-opens=java.base/java.time=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
]


# ---------------------------------------------------------------------------
# locating the vendored dist + extra jars + java
# ---------------------------------------------------------------------------
def flink_home(root: Path | None = None) -> Path | None:
    """Return ``labs/.flink/flink-<ver>`` if it contains the dist scripts."""
    base = (root or repo_root()) / "labs" / ".flink"
    if not base.is_dir():
        return None
    for child in sorted(base.iterdir()):
        if (child / "bin" / "start-cluster.sh").exists():
            return child
    return None


def flink_jars_dir(root: Path | None = None) -> Path:
    return (root or repo_root()) / "labs" / ".jars" / "flink"


# Extra jars fetched by labs/fetch_flink.sh (not in the dist lib/).
EXTRA_JARS = (
    "flink-sql-connector-kafka-5.0.0-2.2.jar",
    "iceberg-flink-runtime-2.2-1.12.0.jar",
    "hadoop-client-api-3.4.2.jar",
    "hadoop-client-runtime-3.4.2.jar",
)


def dist_lib_jars(home: Path) -> list[str]:
    return [str(p) for p in sorted((home / "lib").glob("*.jar"))]


def flink_classpath(root: Path | None = None, *, extra: list[str] | None = None,
                    include: tuple[str, ...] = EXTRA_JARS) -> str | None:
    """Full classpath for embedded jobs: dist lib + the chosen extra jars.

    Returns ``None`` if the dist is absent. Missing *extra* jars are skipped
    (an exercise that doesn't need Kafka/Iceberg still works).
    """
    home = flink_home(root)
    if home is None:
        return None
    cp = dist_lib_jars(home)
    jd = flink_jars_dir(root)
    for name in include:
        p = jd / name
        if p.exists():
            cp.append(str(p))
    if extra:
        cp += [str(e) for e in extra]
    return os.pathsep.join(cp)


def flink_available(root: Path | None = None) -> tuple[bool, str]:
    """(ok, reason). ok iff the dist AND a usable Java exist."""
    if flink_home(root) is None:
        return False, (
            "native Flink not found. Run `bash labs/fetch_flink.sh` to download "
            "the pinned, checksum-verified Flink 2.x dist into labs/.flink/.")
    info = resolve_java()
    if not info["usable"]:
        return False, (f"Java unusable for Flink: {info['reason']} "
                       "Run `bash labs/setup_jdk.sh` to vendor JDK 21.")
    return True, ""


def _java_home() -> str:
    info = resolve_java()
    return info["java_home"] or ""


def _env(extra: dict | None = None) -> dict:
    env = dict(os.environ)
    jh = _java_home()
    if jh:
        env["JAVA_HOME"] = jh
        env["PATH"] = str(Path(jh) / "bin") + os.pathsep + env.get("PATH", "")
    if extra:
        env.update(extra)
    return env


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Embedded Java runner (DataStream / Table API) — compile + run in-process
# ---------------------------------------------------------------------------
@dataclass
class FlinkJavaResult:
    returncode: int
    result: object            # parsed DE_RESULT=... JSON (or None)
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def _collect_java(root: Path) -> list[str]:
    return [str(p) for p in root.rglob("*.java")]


def run_flink_java(submission_dir: str | Path, driver_source: str, *,
                   driver_name: str = "DeDriver",
                   extra_sources: list[str | Path] | None = None,
                   env_vars: dict | None = None,
                   timeout_s: int = 180,
                   root: Path | None = None) -> FlinkJavaResult:
    """Compile the learner's Java + a *driver* and run the driver in-process on
    an embedded Flink MiniCluster. The driver must print exactly one line of the
    form ``DE_RESULT=<json>`` which is parsed and returned as ``.result``.

    * ``driver_source`` — Java source of a ``public class <driver_name>`` with a
      ``public static void main(String[])``. It calls the learner's classes and
      prints ``DE_RESULT=`` + a JSON string.
    * ``extra_sources`` — extra helper ``.java`` files (test harness, fixtures).
    * ``env_vars`` — environment variables the driver reads (e.g. a warehouse
      path, a Kafka bootstrap). ``WAREHOUSE`` etc. are passed through.

    SKIPs cleanly if the dist/JDK are missing. Raising is reserved for genuine
    harness bugs; a learner-code failure comes back as ``returncode != 0`` with
    the compiler/runtime output in ``stderr`` so tests can assert G1.
    """
    import pytest

    ok, reason = flink_available(root)
    if not ok:
        pytest.skip(reason)

    submission_dir = Path(submission_dir)
    home = flink_home(root)
    cp = flink_classpath(root)
    jh = _java_home()
    javac = str(Path(jh) / "bin" / "javac")
    java = str(Path(jh) / "bin" / "java")

    work = Path(tempfile.mkdtemp(prefix="flink-java-"))
    try:
        src = work / "src"
        src.mkdir()
        for jf in _collect_java(submission_dir):
            rel = Path(jf).relative_to(submission_dir)
            dest = src / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(jf, dest)
        (src / f"{driver_name}.java").write_text(driver_source)
        for es in (extra_sources or []):
            es = Path(es)
            shutil.copy2(es, src / es.name)

        classes = work / "classes"
        classes.mkdir()
        all_java = _collect_java(src)
        comp = subprocess.run([javac, "-cp", cp, "-d", str(classes), *all_java],
                              capture_output=True, text=True, timeout=timeout_s,
                              env=_env())
        if comp.returncode != 0:
            return FlinkJavaResult(comp.returncode, None, comp.stdout,
                                   "COMPILE ERROR:\n" + comp.stderr)

        run_cp = cp + os.pathsep + str(classes)
        run = subprocess.run(
            [java, *JAVA17_OPENS, "-cp", run_cp, driver_name],
            capture_output=True, text=True, timeout=timeout_s,
            env=_env(env_vars))
        result = None
        for line in (run.stdout or "").splitlines():
            if line.startswith("DE_RESULT="):
                try:
                    result = json.loads(line[len("DE_RESULT="):])
                except json.JSONDecodeError:
                    result = line[len("DE_RESULT="):]
        return FlinkJavaResult(run.returncode, result, run.stdout, run.stderr)
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# Flink SQL runner (embedded TableEnvironment; returns changelog + RowKind)
# ---------------------------------------------------------------------------
# A tiny Java harness: reads a JSON spec from argv[0] = a file containing
# {"mode":"streaming"|"batch","ddl":[...],"query":"SELECT ...","limit":N} and
# prints DE_RESULT=[["+I", "col0", "col1"], ...] (RowKind short string + fields).
_SQL_HARNESS = r"""
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import java.nio.file.*;
import java.util.*;

public class DeSqlHarness {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        String specPath = args[0];
        String raw = new String(Files.readAllBytes(Paths.get(specPath)));
        Map<String,Object> spec = parse(raw);
        String mode = (String) spec.getOrDefault("mode", "streaming");
        EnvironmentSettings settings = "batch".equals(mode)
            ? EnvironmentSettings.inBatchMode() : EnvironmentSettings.inStreamingMode();
        TableEnvironment t = TableEnvironment.create(settings);
        for (Object stmt : (List<Object>) spec.getOrDefault("ddl", new ArrayList<>())) {
            t.executeSql((String) stmt);
        }
        List<String> inserts = (List<String>) spec.getOrDefault("inserts", new ArrayList<>());
        for (String ins : inserts) t.executeSql(ins).await();
        String query = (String) spec.get("query");
        int limit = ((Number) spec.getOrDefault("limit", 1000)).intValue();
        TableResult r = t.executeSql(query);
        List<List<String>> out = new ArrayList<>();
        try (CloseableIterator<Row> it = r.collect()) {
            int n = 0;
            while (it.hasNext() && n < limit) {
                Row row = it.next();
                List<String> rec = new ArrayList<>();
                rec.add(row.getKind().shortString());
                int ar = row.getArity();
                for (int i = 0; i < ar; i++) {
                    Object f = row.getField(i);
                    rec.add(f == null ? null : String.valueOf(f));
                }
                out.add(rec); n++;
            }
        }
        System.out.println("DE_RESULT=" + toJson(out));
    }

    // --- minimal JSON (we only need maps/lists/strings/ints) ---
    static Map<String,Object> parse(String s) {
        return (Map<String,Object>) new Json(s).value();
    }
    static String toJson(Object o) {
        StringBuilder sb = new StringBuilder(); write(sb, o); return sb.toString();
    }
    static void write(StringBuilder sb, Object o) {
        if (o == null) { sb.append("null"); return; }
        if (o instanceof List) {
            sb.append('['); List<?> l = (List<?>) o;
            for (int i = 0; i < l.size(); i++) { if (i>0) sb.append(','); write(sb, l.get(i)); }
            sb.append(']'); return;
        }
        if (o instanceof String) {
            sb.append('"');
            for (char c : ((String)o).toCharArray()) {
                if (c=='"'||c=='\\') sb.append('\\').append(c);
                else if (c=='\n') sb.append("\\n");
                else if (c=='\r') sb.append("\\r");
                else if (c=='\t') sb.append("\\t");
                else sb.append(c);
            }
            sb.append('"'); return;
        }
        sb.append(String.valueOf(o));
    }
    // tiny recursive-descent JSON parser
    static class Json {
        final String s; int i;
        Json(String s){this.s=s;}
        Object value(){ ws(); char c=s.charAt(i);
            if(c=='{')return obj(); if(c=='[')return arr();
            if(c=='"')return str(); if(c=='t'||c=='f')return bool(); if(c=='n'){i+=4;return null;}
            return num(); }
        void ws(){ while(i<s.length()&&Character.isWhitespace(s.charAt(i)))i++; }
        Map<String,Object> obj(){ Map<String,Object> m=new LinkedHashMap<>(); i++; ws();
            if(s.charAt(i)=='}'){i++;return m;}
            while(true){ ws(); String k=str(); ws(); i++; /*:*/ Object v=value(); m.put(k,v); ws();
                if(s.charAt(i)==','){i++;continue;} i++; break; } return m; }
        List<Object> arr(){ List<Object> l=new ArrayList<>(); i++; ws();
            if(s.charAt(i)==']'){i++;return l;}
            while(true){ Object v=value(); l.add(v); ws();
                if(s.charAt(i)==','){i++;continue;} i++; break; } return l; }
        String str(){ ws(); StringBuilder b=new StringBuilder(); i++; /*"*/
            while(s.charAt(i)!='"'){ char c=s.charAt(i++);
                if(c=='\\'){ char n=s.charAt(i++);
                    switch(n){case 'n':b.append('\n');break;case 't':b.append('\t');break;
                    case 'r':b.append('\r');break;case '"':b.append('"');break;
                    case '\\':b.append('\\');break;case '/':b.append('/');break;default:b.append(n);} }
                else b.append(c); } i++; return b.toString(); }
        Object bool(){ if(s.charAt(i)=='t'){i+=4;return Boolean.TRUE;} i+=5; return Boolean.FALSE; }
        Object num(){ int st=i; while(i<s.length()&&"-+.eE0123456789".indexOf(s.charAt(i))>=0)i++;
            String t=s.substring(st,i); if(t.contains(".")||t.contains("e")||t.contains("E"))
            return Double.parseDouble(t); return Integer.parseInt(t); }
    }
}
"""


@dataclass
class FlinkSqlRunner:
    """Run Flink SQL on an embedded TableEnvironment, returning changelog rows.

    ``run(ddl, query, mode="streaming", inserts=(), limit=1000)`` returns a list
    of rows; each row is ``[kind, f0, f1, ...]`` where ``kind`` is the RowKind
    short string (``"+I"``, ``"-U"``, ``"+U"``, ``"-D"``) and the fields are
    their string renderings (``None`` stays ``None``). Example (FLK-04)::

        rows = flink_sql.run(
            ddl=["CREATE TABLE src (k STRING, v INT) WITH "
                 "('connector'='values','data-id'='X')"],
            query="SELECT k, COUNT(*) c FROM src GROUP BY k")
        kinds = [r[0] for r in rows]   # +I, -U, +U, ...
    """
    _root: Path | None = None
    _cp: str = ""
    _harness_class: str = ""
    _workbase: Path | None = None

    def run(self, ddl=None, query=None, *, mode: str = "streaming",
            inserts=None, limit: int = 1000, timeout_s: int = 180):
        spec = {"mode": mode, "ddl": list(ddl or []),
                "inserts": list(inserts or []), "query": query, "limit": limit}
        jh = _java_home()
        java = str(Path(jh) / "bin" / "java")
        work = Path(tempfile.mkdtemp(prefix="flink-sql-run-", dir=self._workbase))
        try:
            spec_path = work / "spec.json"
            spec_path.write_text(json.dumps(spec))
            run_cp = self._cp + os.pathsep + self._harness_class
            r = subprocess.run(
                [java, *JAVA17_OPENS, "-cp", run_cp, "DeSqlHarness",
                 str(spec_path)],
                capture_output=True, text=True, timeout=timeout_s, env=_env())
            if r.returncode != 0:
                raise RuntimeError(
                    "Flink SQL harness failed:\nSTDOUT:\n" + r.stdout
                    + "\nSTDERR:\n" + r.stderr[-3000:])
            for line in r.stdout.splitlines():
                if line.startswith("DE_RESULT="):
                    return json.loads(line[len("DE_RESULT="):])
            raise RuntimeError("no DE_RESULT from SQL harness:\n" + r.stdout
                               + "\n" + r.stderr[-2000:])
        finally:
            shutil.rmtree(work, ignore_errors=True)


def build_sql_runner(root: Path | None = None) -> FlinkSqlRunner:
    """Compile the SQL harness once and return a ready FlinkSqlRunner. SKIPs
    cleanly when the dist/JDK are missing."""
    import pytest

    ok, reason = flink_available(root)
    if not ok:
        pytest.skip(reason)
    cp = flink_classpath(root)
    jh = _java_home()
    javac = str(Path(jh) / "bin" / "javac")
    base = Path(tempfile.mkdtemp(prefix="flink-sql-harness-"))
    src = base / "DeSqlHarness.java"
    src.write_text(_SQL_HARNESS)
    classes = base / "classes"
    classes.mkdir()
    comp = subprocess.run([javac, "-cp", cp, "-d", str(classes), str(src)],
                          capture_output=True, text=True, timeout=180, env=_env())
    if comp.returncode != 0:  # pragma: no cover - harness is static
        raise RuntimeError("SQL harness failed to compile:\n" + comp.stderr)
    return FlinkSqlRunner(_root=root, _cp=cp, _harness_class=str(classes),
                          _workbase=base)


# ---------------------------------------------------------------------------
# Native local Flink cluster (JobManager + N TaskManagers) with REST helpers
# ---------------------------------------------------------------------------
@dataclass
class _TMProc:
    popen: subprocess.Popen
    conf_dir: Path
    log_dir: Path

    def alive(self) -> bool:
        return self.popen.poll() is None


class LocalFlinkCluster:
    """A running native Flink standalone session cluster for FLK-01/06/11/12.

    Key API (REST-backed; polls, never sleeps for readiness):

      * ``rest_url`` — ``http://127.0.0.1:<port>``
      * ``overview()`` / ``taskmanagers()`` / ``jobs()`` — parsed REST JSON
      * ``num_taskmanagers()`` / ``total_slots()``
      * ``submit_jar(jar_path, entry_class=None, program_args=None,
        parallelism=None)`` -> job_id
      * ``job(job_id)`` — full job detail (vertices, parallelism, state)
      * ``job_plan(job_id)`` — the JobGraph/plan JSON
      * ``vertices(job_id)`` -> list of {id,name,parallelism,...}
      * ``backpressure(job_id, vertex_id)`` — backpressure sample JSON
      * ``checkpoints(job_id)`` — checkpoint stats JSON
      * ``wait_job_state(job_id, state, ...)`` — poll until RUNNING/FINISHED/...
      * ``cancel_with_savepoint(job_id, target_dir)`` -> savepoint path
      * ``kill_taskmanager(i)`` (SIGKILL) + ``wait_taskmanagers(n)`` for recovery
      * ``start_taskmanager()`` — add a TM back
    """

    def __init__(self, home: Path, work: Path, rest_port: int,
                 jm_proc: subprocess.Popen, jm_log: Path, conf_dir: Path):
        self._home = home
        self._work = work
        self.rest_port = rest_port
        self._jm = jm_proc
        self._jm_log = jm_log
        self._conf_dir = conf_dir
        self._tms: list[_TMProc] = []

    # -- REST ---------------------------------------------------------------
    @property
    def rest_url(self) -> str:
        return f"http://127.0.0.1:{self.rest_port}"

    def _get(self, path: str, timeout: float = 10.0):
        import urllib.request
        with urllib.request.urlopen(self.rest_url + path, timeout=timeout) as r:
            return json.loads(r.read())

    def overview(self):
        return self._get("/overview")

    def taskmanagers(self):
        return self._get("/taskmanagers").get("taskmanagers", [])

    def num_taskmanagers(self) -> int:
        try:
            return len(self.taskmanagers())
        except Exception:  # noqa: BLE001
            return 0

    def total_slots(self) -> int:
        return int(self.overview().get("slots-total", 0))

    def jobs(self):
        return self._get("/jobs/overview").get("jobs", [])

    def job(self, job_id: str):
        return self._get(f"/jobs/{job_id}")

    def job_plan(self, job_id: str):
        return self._get(f"/jobs/{job_id}/plan").get("plan", {})

    def vertices(self, job_id: str):
        return self.job(job_id).get("vertices", [])

    def backpressure(self, job_id: str, vertex_id: str):
        return self._get(f"/jobs/{job_id}/vertices/{vertex_id}/backpressure")

    def checkpoints(self, job_id: str):
        return self._get(f"/jobs/{job_id}/checkpoints")

    # -- job submission -----------------------------------------------------
    def upload_jar(self, jar_path: str | Path) -> str:
        """Upload a jar via REST; return the server-side jar id."""
        import urllib.request
        jar_path = Path(jar_path)
        boundary = "----deflinkboundary"
        body = bytearray()
        body += f"--{boundary}\r\n".encode()
        body += (f'Content-Disposition: form-data; name="jarfile"; '
                 f'filename="{jar_path.name}"\r\n').encode()
        body += b"Content-Type: application/x-java-archive\r\n\r\n"
        body += jar_path.read_bytes()
        body += f"\r\n--{boundary}--\r\n".encode()
        req = urllib.request.Request(
            self.rest_url + "/jars/upload", data=bytes(body), method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
        # filename is like "/.../flink-web-upload/<id>_<name>.jar"
        return Path(resp["filename"]).name

    def submit_jar(self, jar_path: str | Path, *, entry_class: str | None = None,
                   program_args: str | None = None,
                   parallelism: int | None = None) -> str:
        import urllib.request
        jar_id = self.upload_jar(jar_path)
        payload: dict = {}
        if entry_class:
            payload["entryClass"] = entry_class
        if program_args:
            payload["programArgs"] = program_args
        if parallelism:
            payload["parallelism"] = parallelism
        req = urllib.request.Request(
            self.rest_url + f"/jars/{jar_id}/run",
            data=json.dumps(payload).encode(), method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read())["jobid"]

    def wait_job_state(self, job_id: str, state: str,
                       timeout: float = 60.0) -> None:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = self.job(job_id).get("state")
                if last == state:
                    return
                if last in ("FAILED", "CANCELED") and state not in (
                        "FAILED", "CANCELED"):
                    raise RuntimeError(f"job {job_id} reached {last}, "
                                       f"expected {state}")
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)
        raise TimeoutError(f"job {job_id} not {state} within {timeout}s "
                           f"(last={last})")

    def cancel_with_savepoint(self, job_id: str, target_dir: str | Path,
                              timeout: float = 90.0) -> str:
        import urllib.request
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(
            self.rest_url + f"/jobs/{job_id}/savepoints",
            data=json.dumps({"target-directory": f"file://{target}",
                             "cancel-job": True}).encode(),
            method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            trigger = json.loads(r.read())["request-id"]
        deadline = time.time() + timeout
        while time.time() < deadline:
            st = self._get(f"/jobs/{job_id}/savepoints/{trigger}")
            status = st.get("status", {}).get("id")
            if status == "COMPLETED":
                op = st.get("operation", {})
                if op.get("failure-cause"):
                    raise RuntimeError(f"savepoint failed: {op['failure-cause']}")
                return op.get("location")
            time.sleep(0.5)
        raise TimeoutError(f"savepoint for {job_id} not done within {timeout}s")

    # -- taskmanager fault injection ---------------------------------------
    def start_taskmanager(self, wait: bool = True) -> None:
        n_before = self.num_taskmanagers()
        proc = self._spawn_tm()
        self._tms.append(proc)
        if wait:
            self.wait_taskmanagers(n_before + 1)

    def kill_taskmanager(self, index: int = -1) -> None:
        """SIGKILL a TaskManager process group (simulate a crash)."""
        tm = self._tms[index]
        try:
            os.killpg(os.getpgid(tm.popen.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            tm.popen.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        self._tms.pop(index)

    def wait_taskmanagers(self, n: int, timeout: float = READY_TIMEOUT_S) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.num_taskmanagers() == n:
                return
            time.sleep(0.5)
        raise TimeoutError(f"expected {n} taskmanagers within {timeout}s; "
                           f"have {self.num_taskmanagers()}")

    # -- lifecycle ----------------------------------------------------------
    def _spawn_tm(self) -> _TMProc:
        log_dir = self._work / f"tm-log-{len(self._tms)}-{free_port()}"
        log_dir.mkdir(parents=True, exist_ok=True)
        env = _env({"FLINK_CONF_DIR": str(self._conf_dir),
                    "FLINK_LOG_DIR": str(log_dir)})
        # taskmanager.sh start-foreground runs in the current process group;
        # start our own session so we can SIGKILL the whole group on crash tests.
        popen = subprocess.Popen(
            [str(self._home / "bin" / "taskmanager.sh"), "start-foreground"],
            env=env, stdout=open(log_dir / "tm.out", "w"),
            stderr=subprocess.STDOUT, start_new_session=True)
        return _TMProc(popen=popen, conf_dir=self._conf_dir, log_dir=log_dir)

    def wait_rest(self, timeout: float = READY_TIMEOUT_S) -> None:
        import urllib.error
        import urllib.request
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(self.rest_url + "/overview",
                                            timeout=3):
                    return
            except (urllib.error.URLError, OSError):
                time.sleep(0.5)
        raise TimeoutError(f"Flink REST not ready within {timeout}s")

    def shutdown(self) -> None:
        for tm in self._tms:
            try:
                os.killpg(os.getpgid(tm.popen.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        try:
            os.killpg(os.getpgid(self._jm.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            self._jm.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        shutil.rmtree(self._work, ignore_errors=True)


def start_local_cluster(n_taskmanagers: int = 1, *, slots_per_tm: int = 2,
                        root: Path | None = None) -> LocalFlinkCluster:
    """Start JobManager + ``n_taskmanagers`` TaskManagers on free ports."""
    import pytest

    ok, reason = flink_available(root)
    if not ok:
        pytest.skip(reason)
    home = flink_home(root)
    work = Path(tempfile.mkdtemp(prefix="flink-cluster-"))
    conf_dir = work / "conf"
    conf_dir.mkdir(parents=True)

    rpc_port = free_port()
    rest_port = free_port()
    opts = " ".join(JAVA17_OPENS)
    (conf_dir / "config.yaml").write_text(
        "env:\n  java:\n    opts:\n      all: \"" + opts + "\"\n"
        "jobmanager:\n"
        "  rpc:\n    address: 127.0.0.1\n    port: " + str(rpc_port) + "\n"
        "  bind-host: 127.0.0.1\n"
        "  memory:\n    process:\n      size: " + HEAP_JM + "\n"
        "rest:\n  address: 127.0.0.1\n  bind-address: 127.0.0.1\n"
        "  port: " + str(rest_port) + "\n"
        "taskmanager:\n  bind-host: 127.0.0.1\n  host: 127.0.0.1\n"
        "  numberOfTaskSlots: " + str(slots_per_tm) + "\n"
        "  memory:\n    process:\n      size: " + HEAP_TM + "\n"
        "parallelism:\n  default: 1\n"
        "execution:\n  checkpointing:\n    interval: 1s\n"
    )
    jm_log = work / "jm-log"
    jm_log.mkdir()
    env = _env({"FLINK_CONF_DIR": str(conf_dir), "FLINK_LOG_DIR": str(jm_log)})
    jm = subprocess.Popen(
        [str(home / "bin" / "jobmanager.sh"), "start-foreground"],
        env=env, stdout=open(jm_log / "jm.out", "w"),
        stderr=subprocess.STDOUT, start_new_session=True)
    cluster = LocalFlinkCluster(home, work, rest_port, jm, jm_log, conf_dir)
    try:
        cluster.wait_rest()
        for _ in range(n_taskmanagers):
            cluster.start_taskmanager(wait=False)
        cluster.wait_taskmanagers(n_taskmanagers)
    except Exception:
        cluster.shutdown()
        raise
    return cluster


# ---------------------------------------------------------------------------
# PyFlink runner (isolated venv; FLK-08 Python side)
# ---------------------------------------------------------------------------
def pyflink_venv(root: Path | None = None) -> Path | None:
    """Return the PyFlink venv's python if present and importable."""
    base = (root or repo_root()) / "labs" / ".flink" / ".pyflink-venv"
    py = base / "bin" / "python"
    return py if py.exists() else None


def pyflink_available(root: Path | None = None) -> tuple[bool, str]:
    py = pyflink_venv(root)
    if py is None:
        return False, ("PyFlink venv not found. Run `bash labs/fetch_flink.sh` "
                       "(it builds labs/.flink/.pyflink-venv). FLK-08 Python side "
                       "SKIPs until present.")
    info = resolve_java()
    if not info["usable"]:
        return False, (f"Java unusable for PyFlink: {info['reason']} "
                       "Run `bash labs/setup_jdk.sh`.")
    return True, ""


@dataclass
class PyFlinkResult:
    returncode: int
    result: object
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def run_pyflink(script_path: str | Path, *, args: list[str] | None = None,
                env_vars: dict | None = None, timeout_s: int = 180,
                root: Path | None = None) -> PyFlinkResult:
    """Run a PyFlink ``script_path`` in the isolated venv on the vendored JDK.

    The script should print one ``DE_RESULT=<json>`` line, parsed into
    ``.result``. SKIPs cleanly when the venv/JDK are missing. A learner-side
    failure comes back as ``returncode != 0`` so a test can assert G1.
    """
    import pytest

    ok, reason = pyflink_available(root)
    if not ok:
        pytest.skip(reason)
    py = pyflink_venv(root)
    r = subprocess.run([str(py), str(script_path), *(args or [])],
                       capture_output=True, text=True, timeout=timeout_s,
                       env=_env(env_vars))
    result = None
    for line in (r.stdout or "").splitlines():
        if line.startswith("DE_RESULT="):
            try:
                result = json.loads(line[len("DE_RESULT="):])
            except json.JSONDecodeError:
                result = line[len("DE_RESULT="):]
    return PyFlinkResult(r.returncode, result, r.stdout, r.stderr)
