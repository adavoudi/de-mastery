"""C3 / M5 — stateful savepoint-upgrade acceptance test (STUB).

Proves a running streaming job can be stopped with a savepoint, upgraded (job
graph changed) and restored with NO data loss. Uses the native ``flink_cluster``
fixture (JobManager + TaskManagers, OS-assigned ports, guaranteed teardown).

It is a STUB: it skips with a precise reason until M5 is implemented, so it never
silently passes. When implemented it must:

  1. submit the mart job to ``flink_cluster()``;
  2. ``cancel_with_savepoint(job, tmp_path/'sp')``;
  3. change the job (e.g. add a column) and restore from the savepoint;
  4. assert the post-restore output == pre-savepoint output + the new events
     (no gap, no dup) via ``sth_oracle``.

Anti-stall: the cluster is torn down by the fixture; confirm no leftover java with
``pgrep -af java`` after the suite.
"""
from __future__ import annotations

import pytest


@pytest.mark.timeout(300)
def test_savepoint_upgrade(flink_cluster, tmp_path):
    pytest.skip("M5 not implemented: take a savepoint with "
                "flink_cluster().cancel_with_savepoint(...), restore the upgraded "
                "job, and assert no data loss via sth_oracle.")
