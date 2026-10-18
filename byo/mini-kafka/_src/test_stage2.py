"""Stage 2 — segmented log on disk with an offset index.

Black-box + storage-internals: we produce records with a real client (that also
exercises stage 3's Produce once implemented) OR, before Produce exists, we drive
the log by starting the broker and letting stage-3 tests populate it. To keep
stage 2 independent of Produce, we assert the ON-DISK structure the broker must
create for a topic-partition data directory: a ``*.log`` segment file and a
matching ``*.index`` file named by base offset, discoverable under the data dir.
"""
import struct
from pathlib import Path

from _broker_harness import produce_sync


def _find_partition_dir(data_root: Path, topic: str, partition: int) -> Path:
    # the broker is launched with --data-dir <wh>/data ; tests pass tmp_path
    # the harness roots the data at <wh>/data.
    candidates = list(data_root.rglob(f"partition-{partition}"))
    for c in candidates:
        if topic in str(c):
            return c
    assert candidates, f"no partition dir found under {data_root}"
    return candidates[0]


def _data_root(broker) -> Path:
    return broker.data_dir / "data"


def test_segment_and_index_files_exist(broker):
    topic = "seg"
    produce_sync(broker.bootstrap, topic, [(b"k", b"v" * 50) for _ in range(20)])
    pdir = _find_partition_dir(_data_root(broker), topic, 0)
    logs = sorted(pdir.glob("*.log"))
    idxs = sorted(pdir.glob("*.index"))
    assert logs, "no .log segment file written"
    assert idxs, "no .index offset-index file written"
    # the first segment is named by its base offset (0), zero-padded
    assert logs[0].stem == idxs[0].stem, "log and index share a base-offset name"
    assert int(logs[0].stem) == 0, "first segment base offset must be 0"


def test_log_grows_and_index_nonempty(broker):
    topic = "seg2"
    produce_sync(broker.bootstrap, topic, [(None, b"x" * 100) for _ in range(30)])
    pdir = _find_partition_dir(_data_root(broker), topic, 0)
    log = sorted(pdir.glob("*.log"))[0]
    idx = sorted(pdir.glob("*.index"))[0]
    assert log.stat().st_size > 0, "log file is empty after producing"
    raw = idx.read_bytes()
    assert len(raw) >= 8 and len(raw) % 8 == 0, \
        "offset index must hold 8-byte (relative_offset, position) entries"
    # the first index entry must point at relative offset 0, byte position 0
    rel0, pos0 = struct.unpack_from(">ii", raw, 0)
    assert rel0 == 0 and pos0 == 0, "first index entry should map offset 0 -> position 0"


def test_index_entries_point_into_the_log(broker):
    topic = "seg3"
    produce_sync(broker.bootstrap, topic, [(None, b"y" * 20) for _ in range(40)])
    pdir = _find_partition_dir(_data_root(broker), topic, 0)
    log = sorted(pdir.glob("*.log"))[0]
    idx = sorted(pdir.glob("*.index"))[0]
    log_size = log.stat().st_size
    raw = idx.read_bytes()
    for i in range(0, len(raw), 8):
        rel, pos = struct.unpack_from(">ii", raw, i)
        assert 0 <= pos < log_size, f"index position {pos} outside log of {log_size} bytes"
        assert rel >= 0, "relative offsets are non-negative"
