"""Shared pytest conftest for a mini-Kafka stage. Puts this tests/ dir on
sys.path so ``import _broker_harness`` works, and exposes broker fixtures.
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pytest
from _broker_harness import MiniBroker


@pytest.fixture
def broker(tmp_path):
    """A fresh broker per test (its own data dir + random port)."""
    b = MiniBroker(tmp_path / "wh").start()
    try:
        yield b
    finally:
        b.stop()


@pytest.fixture
def make_broker():
    """Factory: start extra brokers (e.g. restart-with-same-data-dir tests)."""
    started = []

    def _make(data_dir=None, node_id=1):
        d = Path(data_dir) if data_dir else Path(tempfile.mkdtemp(prefix="mk-"))
        b = MiniBroker(d, node_id=node_id).start()
        started.append(b)
        return b

    yield _make
    for b in started:
        b.stop()
