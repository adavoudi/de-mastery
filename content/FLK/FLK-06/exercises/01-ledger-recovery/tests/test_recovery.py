"""FLK-06 — survive a TaskManager crash with exactly-once ledger invariants.

The learner's job is built into a jar and submitted to a REAL native local
cluster (JobManager + 2 TaskManagers). We wait for the first checkpoint, SIGKILL
a TaskManager (a genuine crash), let the job recover from its last checkpoint and
finish, then read the COMMITTED output and assert the ledger invariants:

    no loss        — every id 0..N-1 present
    no duplication — each id exactly once
    balanced       — committed count == N

Invariants, never wall-clock (docs/TESTING.md fault-tolerance). SKIPs cleanly if
the vendored Flink dist / JDK is absent (G1 caveat).

G1: the starter has no checkpointing and no commit-on-checkpoint sink, so after
the crash the committed output is incomplete and the invariants fail.
"""

import glob
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

N = 240


def _build_jar(fs, submission_dir, work):
    """Compile every .java under submission_dir into a runnable jar."""
    jh = fs._java_home()
    cp = fs.flink_classpath()
    classes = work / "classes"
    classes.mkdir()
    sources = [str(p) for p in Path(submission_dir).rglob("*.java")]
    comp = subprocess.run(
        [str(Path(jh) / "bin" / "javac"), "-cp", cp, "-d", str(classes), *sources],
        capture_output=True, text=True, timeout=180, env=fs._env())
    assert comp.returncode == 0, "COMPILE ERROR:\n" + comp.stderr
    jar = work / "job.jar"
    subprocess.run(
        [str(Path(jh) / "bin" / "jar"), "cfe", str(jar), "de.flk.LedgerJob",
         "-C", str(classes), "."],
        check=True, env=fs._env())
    return jar


def _submit(cluster, jar, out_dir):
    """Submit with programArgsList so argv reaches the job (Flink 2.x)."""
    import json
    jar_id = cluster.upload_jar(jar)
    payload = {"entryClass": "de.flk.LedgerJob",
               "programArgsList": ["file://" + str(out_dir), str(N)]}
    req = urllib.request.Request(
        cluster.rest_url + f"/jars/{jar_id}/run",
        data=json.dumps(payload).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())["jobid"]


def _committed_ids(out_dir):
    files = [f for f in glob.glob(str(out_dir) + "/**/*", recursive=True)
             if os.path.isfile(f) and not os.path.basename(f).startswith(".")]
    ids = []
    for f in files:
        with open(f) as fh:
            ids += [int(x) for x in fh.read().split()]
    return ids


def _run_with_crash(fs, submission_dir, *, kill=True):
    work = Path(tempfile.mkdtemp(prefix="flk06-"))
    out = work / "out"
    out.mkdir()
    jar = _build_jar(fs, submission_dir, work)
    cluster = fs.start_local_cluster(n_taskmanagers=2, slots_per_tm=1)
    try:
        job_id = _submit(cluster, jar, out)
        cluster.wait_job_state(job_id, "RUNNING", timeout=90)
        if kill:
            # Kill mid-run. Prefer to wait for one COMPLETED checkpoint (so a
            # correct job has a consistent cut to recover from); if none appears
            # within a bounded grace (e.g. a job that never checkpoints), kill
            # anyway while the job is still running.
            deadline = time.time() + 3
            while time.time() < deadline:
                done = cluster.checkpoints(job_id).get("counts", {}).get("completed", 0)
                if done >= 1:
                    break
                time.sleep(0.3)
            cluster.kill_taskmanager()            # SIGKILL — a real crash
            try:
                cluster.start_taskmanager()       # give the job slots to recover
                cluster.wait_taskmanagers(2)
            except Exception:  # noqa: BLE001
                pass
        cluster.wait_job_state(job_id, "FINISHED", timeout=180)
        return _committed_ids(out)
    finally:
        cluster.shutdown()
        shutil.rmtree(work, ignore_errors=True)


def _fs_or_skip():
    from de_testkit import flink_support as fs
    ok, reason = fs.flink_available()
    if not ok:
        pytest.skip(reason)
    return fs


def test_exactly_once_after_taskmanager_crash(submission_dir):
    fs = _fs_or_skip()
    ids = _run_with_crash(fs, submission_dir, kill=True)
    # no loss
    assert set(ids) == set(range(N)), (
        f"missing={sorted(set(range(N)) - set(ids))[:10]} "
        f"extra={sorted(set(ids) - set(range(N)))[:10]}")
    # no duplication
    assert len(ids) == len(set(ids)), (
        f"duplicates present: {len(ids)} committed, {len(set(ids))} distinct")
    # balanced
    assert len(ids) == N, f"committed {len(ids)} != {N}"


def test_exactly_once_without_crash(submission_dir):
    # Sanity: a clean run (no kill) must also be exactly-once; kills the idea
    # that the job only works by luck when crashed.
    fs = _fs_or_skip()
    ids = _run_with_crash(fs, submission_dir, kill=False)
    assert sorted(ids) == list(range(N)), "clean run must be exactly-once too"
