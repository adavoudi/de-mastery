"""Phase 4 platform proof: the Flink self-test exercise passes all gates, and the
embedded SQL runner, the native local cluster, and the PyFlink runner all work
end to end.

All Flink tests SKIP cleanly (never fail) when the vendored dist / JDK / PyFlink
venv are missing — so a bare machine stays green while a provisioned machine
(this one) exercises them for real.
"""

import json
from pathlib import Path

import pytest

from de_cli.content import find_exercise
from de_cli.validate import validate_exercise

FIXTURE_CONTENT = Path(__file__).parent / "fixtures" / "content"


# ---------------------------------------------------------------------------
# layer-0: fetch script + vendored artifacts present (informational)
# ---------------------------------------------------------------------------
def test_fetch_flink_script_exists():
    labs = Path(__file__).parents[2] / "labs"
    assert (labs / "fetch_flink.sh").exists()
    assert (labs / "smoke_flink.sh").exists()


# ---------------------------------------------------------------------------
# layer-1: the embedded-Java self-test exercise passes all gates (G1-G5)
# ---------------------------------------------------------------------------
def test_flink_java_selftest_all_gates():
    from de_testkit import flink_support as fs
    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    ex = find_exercise("ZZZF-01/01-keyed-sum", FIXTURE_CONTENT)
    assert ex is not None
    result = validate_exercise(ex, runs=3)
    failed = [(g.gate, g.detail) for g in result.gates if not g.passed]
    assert result.ok, f"flink self-test gates failed: {failed}"
    assert {"G1", "G2", "G3", "G4", "G5"} == {g.gate for g in result.gates}


def test_flink_java_nested_entry_resolves():
    ex = find_exercise("ZZZF-01/01-keyed-sum", FIXTURE_CONTENT)
    assert ex is not None
    assert ex.entry == "src/main/java/de/selftest/KeyedSum.java"


# ---------------------------------------------------------------------------
# layer-2: embedded Flink SQL runner returns changelog rows with RowKind
# ---------------------------------------------------------------------------
def test_flink_sql_changelog_rowkind():
    from de_testkit import flink_support as fs
    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    runner = fs.build_sql_runner()
    # an updating aggregation over a bounded values source -> +I then -U/+U
    ddl = [
        "CREATE TABLE src (k STRING, v INT) WITH ('connector'='datagen',"
        "'number-of-rows'='3','fields.k.length'='1','fields.v.min'='1',"
        "'fields.v.max'='1')"
    ]
    rows = runner.run(ddl=ddl, query="SELECT k, COUNT(*) c FROM src GROUP BY k",
                      mode="streaming", limit=50)
    assert rows, "expected changelog rows"
    kinds = {r[0] for r in rows}
    assert kinds, kinds
    # RowKind short strings must be from the valid set
    assert kinds <= {"+I", "-U", "+U", "-D"}


def test_flink_sql_batch_group_by():
    from de_testkit import flink_support as fs
    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    runner = fs.build_sql_runner()
    ddl = [
        "CREATE TABLE t (k STRING, v INT) WITH ('connector'='values',"
        "'data-id'='nope')"
    ]
    # use a literal VALUES query instead (no external connector needed)
    rows = runner.run(
        ddl=[],
        query="SELECT k, SUM(v) FROM (VALUES ('x',3),('y',5),('x',4)) "
              "AS t(k,v) GROUP BY k",
        mode="batch", limit=50)
    got = {r[1]: int(r[2]) for r in rows if r[0] == "+I"}
    assert got == {"x": 7, "y": 5}, got


# ---------------------------------------------------------------------------
# layer-3: native local cluster — JM + TMs, REST, submit a jar, recover a TM
# ---------------------------------------------------------------------------
def test_native_cluster_rest_and_slots():
    from de_testkit import flink_support as fs
    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    c = fs.start_local_cluster(n_taskmanagers=1, slots_per_tm=2)
    try:
        ov = c.overview()
        assert ov.get("flink-version", "").startswith("2.")
        assert c.num_taskmanagers() == 1
        assert c.total_slots() == 2
    finally:
        c.shutdown()


def test_native_cluster_kill_taskmanager_recovers():
    from de_testkit import flink_support as fs
    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    c = fs.start_local_cluster(n_taskmanagers=2, slots_per_tm=1)
    try:
        assert c.num_taskmanagers() == 2
        c.kill_taskmanager()               # SIGKILL one TM
        c.wait_taskmanagers(1)             # the survivor is still registered
        c.start_taskmanager()              # add one back
        c.wait_taskmanagers(2)
    finally:
        c.shutdown()


def test_native_cluster_submit_jar():
    """Build a trivial Flink jar, submit it via REST, see it FINISH."""
    import os
    import shutil
    import subprocess
    import tempfile

    from de_testkit import flink_support as fs
    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    home = fs.flink_home()
    jh = fs._java_home()
    cp = fs.flink_classpath()

    work = Path(tempfile.mkdtemp(prefix="flink-jar-"))
    try:
        src = work / "Mini.java"
        src.write_text(
            "import org.apache.flink.streaming.api.environment."
            "StreamExecutionEnvironment;\n"
            "public class Mini {\n"
            "  public static void main(String[] a) throws Exception {\n"
            "    var env = StreamExecutionEnvironment.getExecutionEnvironment();\n"
            "    env.fromData(1,2,3).map(x -> x + 1).print();\n"
            "    env.execute(\"mini\");\n"
            "  }\n}\n")
        classes = work / "classes"
        classes.mkdir()
        comp = subprocess.run(
            [str(Path(jh) / "bin" / "javac"), "-cp", cp, "-d", str(classes),
             str(src)], capture_output=True, text=True, timeout=120,
            env=fs._env())
        assert comp.returncode == 0, comp.stderr
        jar = work / "mini.jar"
        subprocess.run([str(Path(jh) / "bin" / "jar"), "cfe", str(jar), "Mini",
                        "-C", str(classes), "."], check=True, env=fs._env())
        c = fs.start_local_cluster(n_taskmanagers=1, slots_per_tm=2)
        try:
            job_id = c.submit_jar(jar, entry_class="Mini")
            c.wait_job_state(job_id, "FINISHED", timeout=60)
        finally:
            c.shutdown()
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# layer-4: PyFlink runner (isolated venv)
# ---------------------------------------------------------------------------
def test_pyflink_runner():
    from de_testkit import flink_support as fs
    ok, reason = fs.pyflink_available()
    if not ok:
        pytest.skip(reason)
    import tempfile
    work = Path(tempfile.mkdtemp(prefix="pyflink-"))
    try:
        script = work / "job.py"
        script.write_text(
            "import json\n"
            "from pyflink.table import EnvironmentSettings, TableEnvironment\n"
            "t = TableEnvironment.create(EnvironmentSettings.in_batch_mode())\n"
            "res = t.execute_sql(\"SELECT k, SUM(v) FROM "
            "(VALUES ('x',3),('y',5),('x',4)) AS t(k,v) GROUP BY k\")\n"
            "out = {}\n"
            "for r in res.collect():\n"
            "    out[r[0]] = int(r[1])\n"
            "print('DE_RESULT=' + json.dumps(out, sort_keys=True))\n")
        r = fs.run_pyflink(script)
        assert r.ok, f"pyflink rc={r.returncode}\n{r.stderr[-2000:]}"
        assert r.result == {"x": 7, "y": 5}, r.result
    finally:
        import shutil
        shutil.rmtree(work, ignore_errors=True)
