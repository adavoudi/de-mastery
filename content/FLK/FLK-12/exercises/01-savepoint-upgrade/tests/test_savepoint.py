"""FLK-12 — savepoint upgrade preserves keyed state (native local cluster).

We build a jar from the learner's submission, run it on a real local Flink
cluster, take a savepoint at count=5, then restart the SAME jar FROM the
savepoint with the source extended to 10. If the counter's state was truly
restored, the final count is 10; if it was lost (e.g. kept in an instance
field), it is 5. The assertion is the no-loss invariant (==10), polled via the
REST job state + the output file — no sleep() for correctness.

SKIPs cleanly when the Flink dist/JDK are absent (the G1 caveat).
"""

import glob
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest

from de_testkit.flink_support import (
    flink_available, flink_classpath, _env, _java_home,
)


def _build_jar(submission_dir: Path, work: Path) -> Path:
    """Compile every .java in the submission against the Flink classpath and
    package a runnable jar with de.flk.Job as the entry point."""
    cp = flink_classpath()
    jh = _java_home()
    javac = str(Path(jh) / "bin" / "javac")
    jar = str(Path(jh) / "bin" / "jar")
    classes = work / "classes"
    classes.mkdir(parents=True, exist_ok=True)
    sources = glob.glob(str(submission_dir / "**" / "*.java"), recursive=True)
    comp = subprocess.run([javac, "-cp", cp, "-d", str(classes), *sources],
                          capture_output=True, text=True, timeout=180, env=_env())
    assert comp.returncode == 0, f"compile failed:\n{comp.stderr[-2000:]}"
    jar_path = work / "job.jar"
    r = subprocess.run([jar, "cfe", str(jar_path), "de.flk.Job",
                        "-C", str(classes), "."],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return jar_path


def _run_from(cluster, jar_id, out_path, k, savepoint=None):
    payload = {"entryClass": "de.flk.Job", "parallelism": 1,
               "programArgsList": [str(out_path), str(k)]}
    if savepoint:
        payload["savepointPath"] = savepoint
    req = urllib.request.Request(
        cluster.rest_url + f"/jars/{jar_id}/run",
        data=json.dumps(payload).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())["jobid"]


def _poll_file(out_path: Path, pred, timeout=45.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        if out_path.exists():
            try:
                last = int(out_path.read_text().strip())
                if pred(last):
                    return last
            except ValueError:
                pass
        time.sleep(0.2)
    return last


def test_savepoint_preserves_state(flink_cluster, submission_dir, tmp_path):
    ok, reason = flink_available()
    if not ok:
        pytest.skip(reason)

    jar = _build_jar(Path(submission_dir), tmp_path / "build")
    out = tmp_path / "count.txt"

    cluster = flink_cluster(n_taskmanagers=1, slots_per_tm=2)
    jar_id = cluster.upload_jar(jar)

    # phase 1: K=5 -> count reaches 5, then savepoint+stop
    job1 = _run_from(cluster, jar_id, out, 5)
    cluster.wait_job_state(job1, "RUNNING", timeout=60)
    v1 = _poll_file(out, lambda v: v >= 5, timeout=45)
    assert v1 == 5, f"phase 1 should reach count 5, got {v1}"
    sp = cluster.cancel_with_savepoint(job1, tmp_path / "sp", timeout=90)
    assert sp, "no savepoint produced"

    # phase 2: restore from savepoint, K=10 -> source emits 5 more.
    job2 = _run_from(cluster, jar_id, out, 10, savepoint=sp)
    cluster.wait_job_state(job2, "RUNNING", timeout=60)
    v2 = _poll_file(out, lambda v: v >= 10, timeout=45)
    assert v2 == 10, (
        f"after restoring from the savepoint the count must continue to 10 "
        f"(state preserved); got {v2} — a reset would be 5")
