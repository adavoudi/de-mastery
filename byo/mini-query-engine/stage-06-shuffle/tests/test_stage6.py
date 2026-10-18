"""Stage 6 — partitioned execution with a hash shuffle (multiprocessing).

``execute_parallel`` runs a hash-aggregate as shuffled, partitioned work across
processes and must equal the single-process aggregate / DuckDB (G6). Two subtle
correctness requirements are tested directly:

* ``stable_hash`` must be deterministic ACROSS processes (Python's built-in
  ``hash()`` is salted per interpreter via PYTHONHASHSEED), so a child process
  computes the same partition as the parent.
* Every row of a given group must land in exactly one partition, so unioning the
  per-partition partials yields the correct total.
"""

import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from de_testkit import assert_frames_equal


def _write(path, n=600, rg=100):
    ids = list(range(n))
    cats = [f"k{i % 23}" for i in ids]  # 23 groups
    t = pa.table({
        "cat": pa.array(cats),
        "amt": pa.array([float((i * 13) % 100) for i in ids], pa.float64()),
    })
    pq.write_table(t, path, row_group_size=rg)
    return t


def test_parallel_aggregate_matches_duckdb(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "s.parquet")
    _write(p)
    plan = submission.Aggregate(
        submission.new_scan(p), keys=["cat"],
        aggs=(("sum", "amt", "s"), ("count", None, "n"), ("avg", "amt", "av")))
    out = submission.execute_parallel(plan, num_partitions=4, processes=2)
    expected = duckdb_con.execute(
        f"SELECT cat, sum(amt) AS s, count(*) AS n, avg(amt) AS av "
        f"FROM read_parquet('{p}') GROUP BY cat").arrow()
    assert_frames_equal(out, expected)


def test_parallel_equals_single_process(submission, tmp_path):
    p = str(tmp_path / "s.parquet")
    _write(p)
    plan = submission.Aggregate(
        submission.new_scan(p), keys=["cat"], aggs=(("sum", "amt", "s"),))
    serial = submission.execute(plan)
    parallel = submission.execute_parallel(plan, num_partitions=4, processes=2)
    assert_frames_equal(parallel, serial)


def test_hash_partition_disjoint_and_complete(submission, tmp_path):
    p = str(tmp_path / "s.parquet")
    t = _write(p, n=300)
    parts = submission.hash_partition(t, ["cat"], 4)
    assert len(parts) == 4
    total = sum(part.num_rows for part in parts)
    assert total == t.num_rows  # no rows lost or duplicated
    # Each group key appears in exactly one partition.
    seen = {}
    for pi, part in enumerate(parts):
        for k in set(part.column("cat").to_pylist()):
            assert k not in seen, f"group {k} split across partitions"
            seen[k] = pi


def test_stable_hash_is_cross_process_deterministic(submission, tmp_path):
    """A fresh child interpreter (different PYTHONHASHSEED) must compute the same
    stable_hash as this process. This is the whole point of the shuffle: a salted
    hash() would scatter the same key to different partitions in different workers.
    """
    import os

    sub_dir = os.environ["DE_SUBMISSION_DIR"]
    entry = os.environ.get("DE_SUBMISSION_ENTRY", "engine.py")
    engine_path = os.path.join(sub_dir, entry)
    local = submission.stable_hash(("k7",))
    script = tmp_path / "child.py"
    script.write_text(
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('m', {engine_path!r})\n"
        "m = importlib.util.module_from_spec(spec)\n"
        "sys.modules['m'] = m\n"
        "spec.loader.exec_module(m)\n"
        "print(m.stable_hash(('k7',)))\n"
    )
    # PYTHONHASHSEED differs from the parent -> exposes any use of hash().
    out = subprocess.run([sys.executable, str(script)],
                         capture_output=True, text=True,
                         env={**os.environ, "PYTHONHASHSEED": "12345"})
    assert out.returncode == 0, out.stderr
    child = int(out.stdout.strip())
    assert child == local, "stable_hash is not deterministic across processes"
