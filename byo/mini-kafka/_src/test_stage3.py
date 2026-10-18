"""Stage 3 — Produce (single partition): RecordBatch v2 + CRC32C.

Black-box: a real librdkafka Producer sends records; the broker must assign
contiguous base offsets and persist the record batches. Storage-internals: we
read the raw ``.log`` back and verify the stored v2 RecordBatch has a correct
CRC32C and the record count / magic byte the client sent.
"""
import struct
from pathlib import Path

from _broker_harness import produce_sync

# ---- a self-contained CRC32C to validate what the broker stored -----------
_T = None


def _crc32c(data: bytes) -> int:
    global _T
    if _T is None:
        poly = 0x82F63B78; t = []
        for n in range(256):
            c = n
            for _ in range(8):
                c = (c >> 1) ^ poly if (c & 1) else (c >> 1)
            t.append(c)
        _T = t
    crc = 0xFFFFFFFF
    for b in data:
        crc = (crc >> 8) ^ _T[(crc ^ b) & 0xFF]
    return crc ^ 0xFFFFFFFF


def _data_root(broker):
    return broker.data_dir / "data"


def _log_bytes(broker, topic, partition=0):
    pdir = next(p for p in _data_root(broker).rglob(f"partition-{partition}")
                if topic in str(p))
    return b"".join(f.read_bytes() for f in sorted(pdir.glob("*.log")))


def _iter_batches(raw):
    i, n = 0, len(raw)
    while i + 12 <= n:
        base = struct.unpack_from(">q", raw, i)[0]
        blen = struct.unpack_from(">i", raw, i + 8)[0]
        total = 12 + blen
        if blen <= 0 or i + total > n:
            break
        yield base, raw[i:i + total]
        i += total


def test_produce_assigns_contiguous_offsets(broker):
    topic = "prod"
    offsets = produce_sync(broker.bootstrap, topic,
                           [(f"k{i}".encode(), f"v{i}".encode()) for i in range(6)])
    assert offsets == [0, 1, 2, 3, 4, 5], f"expected 0..5, got {offsets}"


def test_second_produce_continues_offsets(broker):
    topic = "prod2"
    produce_sync(broker.bootstrap, topic, [(None, b"a"), (None, b"b")])
    offs = produce_sync(broker.bootstrap, topic, [(None, b"c"), (None, b"d")])
    assert offs == [2, 3], f"offsets must continue from 2, got {offs}"


def test_stored_batch_is_v2_with_valid_crc(broker):
    topic = "prodcrc"
    produce_sync(broker.bootstrap, topic, [(b"k", b"hello"), (b"k", b"world")])
    raw = _log_bytes(broker, topic)
    assert raw, "nothing persisted to the log"
    batches = list(_iter_batches(raw))
    assert batches, "no record batch found on disk"
    for base, bb in batches:
        magic = bb[16]
        assert magic == 2, f"RecordBatch magic must be 2 (v2), got {magic}"
        stored_crc = struct.unpack_from(">I", bb, 17)[0]
        after_crc = bb[21:]
        assert _crc32c(after_crc) == stored_crc, \
            "CRC32C over the bytes after the crc field must match the stored crc"


def test_base_offset_stamped_on_disk(broker):
    topic = "prodbase"
    produce_sync(broker.bootstrap, topic, [(None, b"1"), (None, b"2")])
    produce_sync(broker.bootstrap, topic, [(None, b"3")])
    raw = _log_bytes(broker, topic)
    bases = [base for base, _ in _iter_batches(raw)]
    # the broker must stamp each stored batch's baseOffset with the offset it
    # assigned: first batch 0, next batch 2.
    assert bases == [0, 2], f"stored base offsets wrong: {bases}"
