#!/usr/bin/env python3
"""Mini Kafka — a from-scratch broker that speaks the real Kafka wire protocol.

CUMULATIVE reference implementation for the KFK-BYO challenge (all 8 stages).
Python stdlib only (asyncio + struct).

CLI:
    python broker.py --data-dir DIR [--host 127.0.0.1] [--port 0] [--node-id 1]
Prints ``LISTENING <host>:<port>`` on stdout once bound (readiness, no sleep).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import struct
import time
from pathlib import Path

PRODUCE = 0
FETCH = 1
LIST_OFFSETS = 2
METADATA = 3
OFFSET_COMMIT = 8
OFFSET_FETCH = 9
FIND_COORDINATOR = 10
API_VERSIONS = 18
CREATE_TOPICS = 19

SUPPORTED = {
    PRODUCE: (0, 11),
    FETCH: (0, 16),
    LIST_OFFSETS: (0, 8),
    METADATA: (0, 12),
    OFFSET_COMMIT: (0, 9),
    OFFSET_FETCH: (0, 9),
    FIND_COORDINATOR: (0, 4),
    API_VERSIONS: (0, 3),
    CREATE_TOPICS: (0, 7),
}
FLEXIBLE_FROM = {
    PRODUCE: 9, FETCH: 12, LIST_OFFSETS: 6, METADATA: 9,
    OFFSET_COMMIT: 8, OFFSET_FETCH: 6, FIND_COORDINATOR: 3, API_VERSIONS: 3,
    CREATE_TOPICS: 5,
}
NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3


class Reader:
    def __init__(self, buf):
        self.b = buf; self.i = 0

    def _take(self, n):
        v = self.b[self.i:self.i + n]; self.i += n; return v

    def int8(self):
        v = struct.unpack_from(">b", self.b, self.i)[0]; self.i += 1; return v

    def int16(self):
        v = struct.unpack_from(">h", self.b, self.i)[0]; self.i += 2; return v

    def int32(self):
        v = struct.unpack_from(">i", self.b, self.i)[0]; self.i += 4; return v

    def int64(self):
        v = struct.unpack_from(">q", self.b, self.i)[0]; self.i += 8; return v

    def unsigned_varint(self):
        res = 0; shift = 0
        while True:
            byte = self.b[self.i]; self.i += 1
            res |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return res
            shift += 7

    def string(self):
        n = self.int16()
        return None if n < 0 else self._take(n).decode()

    def compact_string(self):
        n = self.unsigned_varint()
        return None if n == 0 else self._take(n - 1).decode()

    def nullable_string(self, flexible):
        return self.compact_string() if flexible else self.string()

    def bytes_(self):
        n = self.int32()
        return None if n < 0 else self._take(n)

    def compact_bytes(self):
        n = self.unsigned_varint()
        return None if n == 0 else self._take(n - 1)

    def tagged_fields(self):
        for _ in range(self.unsigned_varint()):
            self.unsigned_varint()
            self._take(self.unsigned_varint())


class Writer:
    def __init__(self):
        self.out = bytearray()

    def raw(self, b): self.out += b
    def int8(self, v): self.out += struct.pack(">b", v)
    def int16(self, v): self.out += struct.pack(">h", v)
    def int32(self, v): self.out += struct.pack(">i", v)
    def int64(self, v): self.out += struct.pack(">q", v)

    def unsigned_varint(self, v):
        while True:
            if (v & ~0x7F) == 0:
                self.out.append(v); return
            self.out.append((v & 0x7F) | 0x80); v >>= 7

    def string(self, s):
        if s is None:
            self.int16(-1)
        else:
            e = s.encode(); self.int16(len(e)); self.out += e

    def compact_string(self, s):
        if s is None:
            self.unsigned_varint(0)
        else:
            e = s.encode(); self.unsigned_varint(len(e) + 1); self.out += e

    def nullable_string(self, s, flexible):
        self.compact_string(s) if flexible else self.string(s)

    def bytes_(self, b):
        if b is None:
            self.int32(-1)
        else:
            self.int32(len(b)); self.out += b

    def compact_bytes(self, b):
        if b is None:
            self.unsigned_varint(0)
        else:
            self.unsigned_varint(len(b) + 1); self.out += b

    def empty_tagged_fields(self):
        self.unsigned_varint(0)

    def getvalue(self):
        return bytes(self.out)


_CRC32C_TABLE = None


def _crc32c_table():
    global _CRC32C_TABLE
    if _CRC32C_TABLE is None:
        poly = 0x82F63B78; t = []
        for n in range(256):
            c = n
            for _ in range(8):
                c = (c >> 1) ^ poly if (c & 1) else (c >> 1)
            t.append(c)
        _CRC32C_TABLE = t
    return _CRC32C_TABLE


def crc32c(data: bytes) -> int:
    t = _crc32c_table(); crc = 0xFFFFFFFF
    for b in data:
        crc = (crc >> 8) ^ t[(crc ^ b) & 0xFF]
    return crc ^ 0xFFFFFFFF


INDEX_INTERVAL_BYTES = 4096


class Segment:
    def __init__(self, directory: Path, base_offset: int):
        self.dir = directory
        self.base_offset = base_offset
        self.log_path = directory / f"{base_offset:020d}.log"
        self.index_path = directory / f"{base_offset:020d}.index"
        self.log_path.touch(exist_ok=True)
        self.index_path.touch(exist_ok=True)
        self._bytes_since_index = 0

    def size(self):
        return self.log_path.stat().st_size

    def append_batch(self, raw_batch, base_offset):
        pos = self.size()
        with open(self.log_path, "ab") as f:
            f.write(raw_batch)
        self._bytes_since_index += len(raw_batch)
        if self._bytes_since_index >= INDEX_INTERVAL_BYTES or pos == 0:
            rel = base_offset - self.base_offset
            with open(self.index_path, "ab") as idx:
                idx.write(struct.pack(">ii", rel, pos))
            self._bytes_since_index = 0

    def index_entries(self):
        raw = self.index_path.read_bytes()
        return [struct.unpack_from(">ii", raw, i) for i in range(0, len(raw) - 7, 8)]

    def read_from(self, byte_pos):
        with open(self.log_path, "rb") as f:
            f.seek(byte_pos)
            return f.read()


class PartitionLog:
    SEGMENT_ROLL_BYTES = 1 << 20

    def __init__(self, directory: Path):
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.segments = []
        self.log_end_offset = 0
        self.log_start_offset = 0
        self.high_watermark = 0
        self._load()

    def _load(self):
        bases = sorted(int(p.stem) for p in self.dir.glob("*.log"))
        if not bases:
            self.segments = [Segment(self.dir, 0)]
            return
        self.segments = [Segment(self.dir, b) for b in bases]
        self.log_start_offset = bases[0]
        leo = bases[0]
        for seg in self.segments:
            for _b, last in iter_batches(seg.read_from(0)):
                leo = last + 1
        self.log_end_offset = leo
        self.high_watermark = leo

    @property
    def active(self):
        return self.segments[-1]

    def _maybe_roll(self):
        if self.active.size() >= self.SEGMENT_ROLL_BYTES:
            self.segments.append(Segment(self.dir, self.log_end_offset))

    def append(self, raw_batch, count):
        base = self.log_end_offset
        stamped = stamp_base_offset(raw_batch, base)
        self._maybe_roll()
        self.active.append_batch(stamped, base)
        self.log_end_offset = base + count
        # single-node: the record is durable once written+fsync'd, so the high
        # watermark advances to the log end offset (no followers to wait for).
        self._fsync_active()
        self.high_watermark = max(0, self.log_end_offset - 1)
        return base

    def _fsync_active(self):
        try:
            with open(self.active.log_path, "ab") as f:
                f.flush()
                os.fsync(f.fileno())
        except OSError:
            pass

    def read(self, fetch_offset, max_bytes):
        if fetch_offset >= self.high_watermark:
            return b""
        seg = self.segments[0]
        for s in self.segments:
            if s.base_offset <= fetch_offset:
                seg = s
            else:
                break
        start_pos = 0
        for rel, pos in seg.index_entries():
            if seg.base_offset + rel <= fetch_offset:
                start_pos = pos
            else:
                break
        collected = io.BytesIO()
        collected.write(seg.read_from(start_pos))
        for s in self.segments[self.segments.index(seg) + 1:]:
            collected.write(s.read_from(0))
        out = io.BytesIO(); total = 0
        for batch_bytes, last_off, _base in iter_batches_with_bytes(collected.getvalue()):
            if last_off < fetch_offset:
                continue
            if total and total + len(batch_bytes) > max_bytes:
                break
            out.write(batch_bytes); total += len(batch_bytes)
        return out.getvalue()


CRC_OFFSET = 8 + 4 + 4 + 1


def stamp_base_offset(raw, base):
    b = bytearray(raw)
    struct.pack_into(">q", b, 0, base)
    return bytes(b)


def iter_batches(raw):
    for _, last, base in iter_batches_with_bytes(raw):
        yield base, last


def iter_batches_with_bytes(raw):
    i, n = 0, len(raw)
    while i + 12 <= n:
        base_offset = struct.unpack_from(">q", raw, i)[0]
        batch_len = struct.unpack_from(">i", raw, i + 8)[0]
        total = 12 + batch_len
        if batch_len <= 0 or i + total > n:
            break
        batch_bytes = raw[i:i + total]
        last_delta = struct.unpack_from(">i", raw, i + 23)[0]
        yield batch_bytes, base_offset + last_delta, base_offset
        i += total


def record_count(raw_batch):
    return struct.unpack_from(">i", raw_batch, 57)[0]


def _read_varlong(buf, i):
    res = 0; shift = 0
    while True:
        byte = buf[i]; i += 1
        res |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return (res >> 1) ^ -(res & 1), i


def decode_records(raw_batch, base_offset):
    """Yield (key, value, absolute_offset) for each record in a v2 batch."""
    count = struct.unpack_from(">i", raw_batch, 57)[0]
    i = 61   # start of records section
    n = len(raw_batch)
    for _ in range(count):
        if i >= n:
            break
        length, i = _read_varlong(raw_batch, i)
        end = i + length
        i += 1   # attributes (int8)
        _ts, i = _read_varlong(raw_batch, i)
        offset_delta, i = _read_varlong(raw_batch, i)
        klen, i = _read_varlong(raw_batch, i)
        key = None
        if klen >= 0:
            key = bytes(raw_batch[i:i + klen]); i += klen
        vlen, i = _read_varlong(raw_batch, i)
        value = None
        if vlen >= 0:
            value = bytes(raw_batch[i:i + vlen]); i += vlen
        i = end
        yield key, value, base_offset + offset_delta


def verify_crc(raw_batch):
    stored = struct.unpack_from(">I", raw_batch, CRC_OFFSET)[0]
    return crc32c(raw_batch[CRC_OFFSET + 4:]) == stored


def build_record_batch(records, base_offset=0):
    now = int(time.time() * 1000)
    recs = bytearray()
    for i, (k, v) in enumerate(records):
        rb = bytearray()
        rb.append(0)
        _put_varlong(rb, 0)
        _put_varlong(rb, i)
        _put_varlong(rb, len(k) if k is not None else -1)
        if k is not None:
            rb += k
        _put_varlong(rb, len(v) if v is not None else -1)
        if v is not None:
            rb += v
        _put_varlong(rb, 0)
        length = bytearray(); _put_varlong(length, len(rb))
        recs += length + rb
    body = bytearray()
    body += struct.pack(">h", 0)
    body += struct.pack(">i", len(records) - 1)
    body += struct.pack(">q", now)
    body += struct.pack(">q", now)
    body += struct.pack(">q", -1)
    body += struct.pack(">h", -1)
    body += struct.pack(">i", -1)
    body += struct.pack(">i", len(records))
    body += recs
    crc = crc32c(bytes(body))
    header = struct.pack(">i", -1) + struct.pack(">B", 2) + struct.pack(">I", crc)
    payload = header + bytes(body)
    return struct.pack(">qi", base_offset, len(payload)) + payload


def _put_varlong(buf, v):
    uv = ((v << 1) ^ (v >> 63)) & 0xFFFFFFFFFFFFFFFF
    while True:
        if (uv & ~0x7F) == 0:
            buf.append(uv); return
        buf.append((uv & 0x7F) | 0x80); uv >>= 7

class Broker:
    def __init__(self, host, port, data_dir, node_id=1):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs = {}
        self.topics = {}
        self.topic_configs = {}
        self.group_offsets = {}
        self._load_meta()

    def _meta_path(self):
        return self.data_dir / "topics.json"

    def _config_path(self):
        return self.data_dir / "topic_configs.json"

    def _load_meta(self):
        p = self._meta_path()
        if p.exists():
            self.topics = {k: int(v) for k, v in json.loads(p.read_text()).items()}
        cp = self._config_path()
        if cp.exists():
            self.topic_configs = json.loads(cp.read_text())
        for topic, nparts in self.topics.items():
            for pi in range(nparts):
                self._log(topic, pi)

    def _save_meta(self):
        self._meta_path().write_text(json.dumps(self.topics))
        self._config_path().write_text(json.dumps(self.topic_configs))

    def ensure_topic(self, topic, partitions=1):
        if topic not in self.topics:
            self.topics[topic] = partitions
            for pi in range(partitions):
                self._log(topic, pi)
            self._save_meta()

    def _log(self, topic, partition):
        key = (topic, partition)
        if key not in self.logs:
            d = self.data_dir / topic / f"partition-{partition}"
            self.logs[key] = PartitionLog(d)
        return self.logs[key]

    async def serve(self):
        server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = server.sockets[0].getsockname()[1]
        print(f"LISTENING {self.host}:{self.port}", flush=True)
        async with server:
            await server.serve_forever()

    async def _handle(self, reader, writer):
        try:
            while True:
                size_bytes = await reader.readexactly(4)
                (size,) = struct.unpack(">i", size_bytes)
                payload = await reader.readexactly(size)
                resp = self.dispatch(payload)
                writer.write(struct.pack(">i", len(resp)) + resp)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    def dispatch(self, payload):
        r = Reader(payload)
        api_key = r.int16()
        api_version = r.int16()
        corr_id = r.int32()
        r.string()
        flexible = api_version >= FLEXIBLE_FROM.get(api_key, 99)
        if flexible:
            r.tagged_fields()
        if api_key == API_VERSIONS:
            return self._api_versions(corr_id, api_version)
        if api_key == METADATA:
            return self._metadata(r, corr_id, api_version, flexible)
        if api_key == PRODUCE:
            return self._produce(r, corr_id, api_version, flexible)
        if api_key == FETCH:
            return self._fetch(r, corr_id, api_version, flexible)
        if api_key == LIST_OFFSETS:
            return self._list_offsets(r, corr_id, api_version, flexible)
        if api_key == FIND_COORDINATOR:
            return self._find_coordinator(r, corr_id, api_version, flexible)
        if api_key == OFFSET_COMMIT:
            return self._offset_commit(r, corr_id, api_version, flexible)
        if api_key == OFFSET_FETCH:
            return self._offset_fetch(r, corr_id, api_version, flexible)
        if api_key == CREATE_TOPICS:
            return self._create_topics(r, corr_id, api_version, flexible)
        w = Writer(); w.int32(corr_id); return w.getvalue()

    def _resp_header(self, corr_id, flexible, api_versions_quirk=False):
        w = Writer()
        w.int32(corr_id)
        if flexible and not api_versions_quirk:
            w.empty_tagged_fields()
        return w

    def _api_versions(self, corr_id, api_version):
        flexible = api_version >= FLEXIBLE_FROM[API_VERSIONS]
        w = self._resp_header(corr_id, flexible, api_versions_quirk=True)
        w.int16(NO_ERROR)
        keys = sorted(SUPPORTED.items())
        if flexible:
            w.unsigned_varint(len(keys) + 1)
            for k, (lo, hi) in keys:
                w.int16(k); w.int16(lo); w.int16(hi); w.empty_tagged_fields()
            w.int32(0)
            w.empty_tagged_fields()
        else:
            w.int32(len(keys))
            for k, (lo, hi) in keys:
                w.int16(k); w.int16(lo); w.int16(hi)
            w.int32(0)
        return w.getvalue()

    def _metadata(self, r, corr_id, ver, flexible):
        if flexible:
            n = r.unsigned_varint() - 1
            topics = None if n < 0 else []
            if topics is not None:
                for _ in range(n):
                    if ver >= 10:
                        r._take(16)
                    name = r.compact_string()
                    r.tagged_fields()
                    if name is not None:
                        topics.append(name)
        else:
            n = r.int32()
            topics = None if n < 0 else []
            if topics is not None:
                for _ in range(n):
                    if ver >= 10:
                        r._take(16)
                    name = r.string()
                    if name is not None:
                        topics.append(name)
        want = topics if topics is not None else list(self.topics.keys())
        for t in want:
            if t not in self.topics:
                self.ensure_topic(t, 1)

        w = self._resp_header(corr_id, flexible)
        if ver >= 3:
            w.int32(0)
        self._w_array(w, [None], flexible,
                      lambda ww, _: self._w_broker(ww, ver, flexible))
        if ver >= 2:
            w.nullable_string("mini-kafka-cluster", flexible)
        if ver >= 1:
            w.int32(self.node_id)
        self._w_array(w, want, flexible,
                      lambda ww, t: self._w_topic_md(ww, t, ver, flexible))
        if ver >= 8:
            w.int32(-2147483648)
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _w_broker(self, w, ver, flexible):
        w.int32(self.node_id)
        w.nullable_string(self.host, flexible)
        w.int32(self.port)
        if ver >= 1:
            w.nullable_string(None, flexible)
        if flexible:
            w.empty_tagged_fields()

    def _w_topic_md(self, w, topic, ver, flexible):
        exists = topic in self.topics
        w.int16(NO_ERROR if exists else UNKNOWN_TOPIC_OR_PARTITION)
        w.nullable_string(topic, flexible)
        if ver >= 10:
            w.raw(b"\x00" * 16)
        if ver >= 1:
            w.int8(0)
        nparts = self.topics.get(topic, 0)
        self._w_array(w, list(range(nparts)), flexible,
                      lambda ww, pi: self._w_part_md(ww, pi, ver, flexible))
        if ver >= 8:
            w.int32(-2147483648)
        if flexible:
            w.empty_tagged_fields()

    def _w_part_md(self, w, pi, ver, flexible):
        w.int16(NO_ERROR)
        w.int32(pi)
        w.int32(self.node_id)
        if ver >= 7:
            w.int32(0)
        self._w_array(w, [self.node_id], flexible, lambda ww, nid: ww.int32(nid))
        self._w_array(w, [self.node_id], flexible, lambda ww, nid: ww.int32(nid))
        if ver >= 5:
            self._w_array(w, [], flexible, lambda ww, x: None)
        if flexible:
            w.empty_tagged_fields()

    def _produce(self, r, corr_id, ver, flexible):
        if ver >= 3:
            r.nullable_string(flexible)
        r.int16()   # acks
        r.int32()   # timeout
        results = []
        n_topics = (r.unsigned_varint() - 1) if flexible else r.int32()
        for _ in range(n_topics):
            topic = r.nullable_string(flexible)
            parts = []
            n_parts = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(n_parts):
                pi = r.int32()
                records = r.compact_bytes() if flexible else r.bytes_()
                if flexible:
                    r.tagged_fields()
                base = self._append(topic, pi, records or b"")
                parts.append((pi, base))
            if flexible:
                r.tagged_fields()
            results.append((topic, parts))
        w = self._resp_header(corr_id, flexible)
        self._w_array(w, results, flexible,
                      lambda ww, tr: self._w_produce_topic(ww, tr, ver, flexible))
        if ver >= 1:
            w.int32(0)
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _append(self, topic, pi, records):
        self.ensure_topic(topic, max(pi + 1, self.topics.get(topic, 1)))
        log = self._log(topic, pi)
        if not records:
            return log.log_end_offset
        base = log.log_end_offset
        for batch_bytes, _last, _base in iter_batches_with_bytes(records):
            log.append(batch_bytes, record_count(batch_bytes))
        self._apply_cleanup(topic, pi)
        return base

    def _w_produce_topic(self, w, tr, ver, flexible):
        topic, parts = tr
        w.nullable_string(topic, flexible)
        self._w_array(w, parts, flexible,
                      lambda ww, p: self._w_produce_part(ww, p, ver, flexible))
        if flexible:
            w.empty_tagged_fields()

    def _w_produce_part(self, w, p, ver, flexible):
        pi, base = p
        w.int32(pi)
        w.int16(NO_ERROR)
        w.int64(base)
        if ver >= 2:
            w.int64(-1)
        if ver >= 5:
            w.int64(0)
        if flexible:
            self._w_array(w, [], flexible, lambda ww, x: None)
            w.nullable_string(None, flexible)
            w.empty_tagged_fields()

    def _fetch(self, r, corr_id, ver, flexible):
        if ver < 15:
            r.int32()   # replica_id
        r.int32()       # max_wait_ms
        r.int32()       # min_bytes
        if ver >= 3:
            r.int32()   # max_bytes
        if ver >= 4:
            r.int8()    # isolation_level
        if ver >= 7:
            r.int32(); r.int32()  # session_id, session_epoch
        topics = []
        n_topics = (r.unsigned_varint() - 1) if flexible else r.int32()
        for _ in range(n_topics):
            if ver >= 13:
                topic = None
                r._take(16)
            else:
                topic = r.nullable_string(flexible)
            parts = []
            n_parts = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(n_parts):
                pi = r.int32()
                if ver >= 9:
                    r.int32()
                fetch_offset = r.int64()
                if ver >= 12:
                    r.int32()
                if ver >= 5:
                    r.int64()
                partition_max_bytes = r.int32()
                if flexible:
                    r.tagged_fields()
                parts.append((pi, fetch_offset, partition_max_bytes))
            if flexible:
                r.tagged_fields()
            topics.append((topic, parts))

        w = self._resp_header(corr_id, flexible)
        w.int32(0)
        if ver >= 7:
            w.int16(NO_ERROR)
            w.int32(0)
        self._w_array(w, topics, flexible,
                      lambda ww, tr: self._w_fetch_topic(ww, tr, ver, flexible))
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _resolve_topic_name(self, name):
        if name is not None:
            return name
        names = list(self.topics.keys())
        return names[0] if names else ""

    def _w_fetch_topic(self, w, tr, ver, flexible):
        topic, parts = tr
        name = self._resolve_topic_name(topic)
        if ver >= 13:
            w.raw(b"\x00" * 16)
        else:
            w.nullable_string(name, flexible)
        self._w_array(w, parts, flexible,
                      lambda ww, p: self._w_fetch_part(ww, name, p, ver, flexible))
        if flexible:
            w.empty_tagged_fields()

    def _w_fetch_part(self, w, name, p, ver, flexible):
        pi, fetch_offset, max_bytes = p
        log = self.logs.get((name, pi))
        hw = log.high_watermark if log else 0
        w.int32(pi)
        w.int16(NO_ERROR)
        w.int64(hw)
        if ver >= 4:
            w.int64(hw)
        if ver >= 5:
            w.int64(log.log_start_offset if log else 0)
        if ver >= 4:
            self._w_array(w, [], flexible, lambda ww, x: None)  # aborted_transactions
        if ver >= 11:
            w.int32(-1)   # preferred_read_replica
        records = log.read(fetch_offset, max_bytes) if log else b""
        if flexible:
            w.compact_bytes(records if records else None)
            w.empty_tagged_fields()
        else:
            w.bytes_(records if records else None)

    def _list_offsets(self, r, corr_id, ver, flexible):
        r.int32()
        if ver >= 2:
            r.int8()
        topics = []
        n = (r.unsigned_varint() - 1) if flexible else r.int32()
        for _ in range(n):
            topic = r.nullable_string(flexible)
            parts = []
            np = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(np):
                pi = r.int32()
                if ver >= 4:
                    r.int32()
                ts = r.int64()
                if ver == 0:
                    r.int32()
                if flexible:
                    r.tagged_fields()
                parts.append((pi, ts))
            if flexible:
                r.tagged_fields()
            topics.append((topic, parts))
        w = self._resp_header(corr_id, flexible)
        if ver >= 2:
            w.int32(0)
        self._w_array(w, topics, flexible,
                      lambda ww, tr: self._w_list_topic(ww, tr, ver, flexible))
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _w_list_topic(self, w, tr, ver, flexible):
        topic, parts = tr
        w.nullable_string(topic, flexible)
        self._w_array(w, parts, flexible,
                      lambda ww, p: self._w_list_part(ww, topic, p, ver, flexible))
        if flexible:
            w.empty_tagged_fields()

    def _w_list_part(self, w, topic, p, ver, flexible):
        pi, ts = p
        log = self.logs.get((topic, pi))
        earliest = log.log_start_offset if log else 0
        latest = log.high_watermark if log else 0
        offset = earliest if ts == -2 else latest
        w.int32(pi)
        w.int16(NO_ERROR)
        if ver >= 1:
            w.int64(-1)
        w.int64(offset)
        if ver >= 4:
            w.int32(0)
        if flexible:
            w.empty_tagged_fields()

    def _find_coordinator(self, r, corr_id, ver, flexible):
        keys = []
        if ver >= 4:
            n = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(n):
                keys.append(r.nullable_string(flexible))
                if flexible:
                    r.tagged_fields()
        else:
            keys.append(r.nullable_string(flexible))
            if ver >= 1:
                r.int8()
        w = self._resp_header(corr_id, flexible)
        w.int32(0)
        if ver >= 4:
            self._w_array(w, keys, flexible,
                          lambda ww, key: self._w_coord_entry(ww, key, flexible))
        else:
            w.int16(NO_ERROR)
            if ver >= 1:
                w.nullable_string(None, flexible)
            w.int32(self.node_id)
            w.nullable_string(self.host, flexible)
            w.int32(self.port)
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _w_coord_entry(self, w, key, flexible):
        w.nullable_string(key, flexible)
        w.int16(NO_ERROR)
        w.nullable_string(None, flexible)
        w.int32(self.node_id)
        w.nullable_string(self.host, flexible)
        w.int32(self.port)
        if flexible:
            w.empty_tagged_fields()

    def _offset_commit(self, r, corr_id, ver, flexible):
        group = r.nullable_string(flexible)
        if ver >= 1:
            r.int32()
            r.nullable_string(flexible)
        if ver >= 7:
            r.nullable_string(flexible)
        results = []
        n = (r.unsigned_varint() - 1) if flexible else r.int32()
        for _ in range(n):
            topic = r.nullable_string(flexible)
            parts = []
            np = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(np):
                pi = r.int32()
                committed = r.int64()
                if ver >= 6:
                    r.int32()
                if ver == 1:
                    r.int64()
                r.nullable_string(flexible)
                if flexible:
                    r.tagged_fields()
                self.group_offsets[(group, topic, pi)] = committed
                parts.append(pi)
            if flexible:
                r.tagged_fields()
            results.append((topic, parts))
        w = self._resp_header(corr_id, flexible)
        if ver >= 3:
            w.int32(0)
        self._w_array(w, results, flexible,
                      lambda ww, tr: self._w_commit_topic(ww, tr, flexible))
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _w_commit_topic(self, w, tr, flexible):
        topic, parts = tr
        w.nullable_string(topic, flexible)
        self._w_array(w, parts, flexible,
                      lambda ww, pi: self._w_commit_part(ww, pi, flexible))
        if flexible:
            w.empty_tagged_fields()

    def _w_commit_part(self, w, pi, flexible):
        w.int32(pi)
        w.int16(NO_ERROR)
        if flexible:
            w.empty_tagged_fields()

    def _offset_fetch(self, r, corr_id, ver, flexible):
        groups = []
        if ver >= 8:
            ng = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(ng):
                group = r.nullable_string(flexible)
                if ver >= 9:
                    r.nullable_string(flexible)   # member_id
                    r.int32()                     # member_epoch
                topics = self._read_offset_fetch_topics(r, flexible)
                if flexible:
                    r.tagged_fields()
                groups.append((group, topics))
        else:
            group = r.nullable_string(flexible)
            topics = self._read_offset_fetch_topics(r, flexible)
            groups.append((group, topics))

        w = self._resp_header(corr_id, flexible)
        w.int32(0)
        if ver >= 8:
            self._w_array(w, groups, flexible,
                          lambda ww, g: self._w_offfetch_group(ww, g, ver, flexible))
        else:
            group, topics = groups[0]
            self._w_offfetch_topics(w, group, topics, ver, flexible)
            if ver >= 2:
                w.int16(NO_ERROR)
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _read_offset_fetch_topics(self, r, flexible):
        n = (r.unsigned_varint() - 1) if flexible else r.int32()
        if n < 0:
            return None
        topics = []
        for _ in range(n):
            topic = r.nullable_string(flexible)
            parts = []
            np = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(np):
                parts.append(r.int32())
            if flexible:
                r.tagged_fields()
            topics.append((topic, parts))
        return topics

    def _w_offfetch_group(self, w, g, ver, flexible):
        group, topics = g
        w.nullable_string(group, flexible)
        self._w_offfetch_topics(w, group, topics, ver, flexible)
        w.int16(NO_ERROR)
        if flexible:
            w.empty_tagged_fields()

    def _w_offfetch_topics(self, w, group, topics, ver, flexible):
        if topics is None:
            seen = {}
            for (g, t, pi), _off in self.group_offsets.items():
                if g == group:
                    seen.setdefault(t, []).append(pi)
            topics = [(t, parts) for t, parts in seen.items()]
        self._w_array(w, topics, flexible,
                      lambda ww, tr: self._w_offfetch_topic(ww, group, tr, ver, flexible))

    def _w_offfetch_topic(self, w, group, tr, ver, flexible):
        topic, parts = tr
        w.nullable_string(topic, flexible)
        self._w_array(w, parts, flexible,
                      lambda ww, pi: self._w_offfetch_part(ww, group, topic, pi, ver, flexible))
        if flexible:
            w.empty_tagged_fields()

    def _w_offfetch_part(self, w, group, topic, pi, ver, flexible):
        off = self.group_offsets.get((group, topic, pi), -1)
        w.int32(pi)
        w.int64(off)
        if ver >= 5:
            w.int32(-1)
        w.nullable_string(None, flexible)
        w.int16(NO_ERROR)
        if flexible:
            w.empty_tagged_fields()

    def _create_topics(self, r, corr_id, ver, flexible):
        results = []
        n = (r.unsigned_varint() - 1) if flexible else r.int32()
        for _ in range(n):
            name = r.nullable_string(flexible)
            num_parts = r.int32()
            r.int16()   # replication_factor
            # assignments
            na = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(na):
                r.int32()
                nb = (r.unsigned_varint() - 1) if flexible else r.int32()
                for _ in range(nb):
                    r.int32()
                if flexible:
                    r.tagged_fields()
            configs = {}
            nc = (r.unsigned_varint() - 1) if flexible else r.int32()
            for _ in range(nc):
                ck = r.nullable_string(flexible)
                cv = r.nullable_string(flexible)
                if flexible:
                    r.tagged_fields()
                configs[ck] = cv
            if flexible:
                r.tagged_fields()
            parts = num_parts if num_parts > 0 else 1
            created = name not in self.topics
            self.ensure_topic(name, parts)
            if configs:
                self.topic_configs[name] = {**self.topic_configs.get(name, {}), **configs}
                self._save_meta()
            results.append((name, NO_ERROR if created else 36))  # 36 TOPIC_ALREADY_EXISTS
        w = self._resp_header(corr_id, flexible)
        if ver >= 2:
            w.int32(0)
        self._w_array(w, results, flexible,
                      lambda ww, tr: self._w_create_topic(ww, tr, ver, flexible))
        if flexible:
            w.empty_tagged_fields()
        return w.getvalue()

    def _w_create_topic(self, w, tr, ver, flexible):
        name, err = tr
        w.nullable_string(name, flexible)
        if ver >= 7:
            w.raw(b"\x00" * 16)   # topic_id
        w.int16(err)
        if ver >= 1:
            w.nullable_string(None, flexible)   # error_message
        if ver >= 5:
            w.int16(0)            # topic_config_error_code
            w.int32(self.topics.get(name, 1))   # num_partitions
            w.int16(1)            # replication_factor
            self._w_array(w, [], flexible, lambda ww, x: None)  # configs
        if flexible:
            w.empty_tagged_fields()

    # -- retention + compaction (stage 6), run synchronously after append --
    def _apply_cleanup(self, topic, pi):
        cfg = self.topic_configs.get(topic, {})
        policy = cfg.get("cleanup.policy", "delete")
        log = self.logs.get((topic, pi))
        if log is None:
            return
        if "compact" in policy:
            self._compact(log)
        if "delete" in policy:
            self._retain(log, cfg)

    def _compact(self, log):
        """Keep only the latest record per key across the whole log. Rewrites
        the log into a single compacted segment, preserving absolute offsets."""
        raw = b"".join(s.read_from(0) for s in log.segments)
        latest = {}
        order = []
        for bb, last_off, base_off in iter_batches_with_bytes(raw):
            for key, value, off in decode_records(bb, base_off):
                if key is None:
                    continue   # keep null-key records out of the keyed index
                if key not in latest:
                    order.append(key)
                latest[key] = (value, off)
        # rebuild: one record batch per surviving record, in offset order,
        # preserving each record's original absolute offset as its base offset.
        survivors = sorted(((off, key, value) for key, (value, off) in latest.items()))
        self._rewrite_log(log, survivors)

    def _retain(self, log, cfg):
        retain_ms = cfg.get("retention.ms")
        retain_bytes = cfg.get("retention.bytes")
        # we don't drop by time deterministically; support size-based retention:
        # keep only the last N bytes worth of segments (drop whole old segments).
        if retain_bytes is None:
            return
        try:
            limit = int(retain_bytes)
        except (TypeError, ValueError):
            return
        total = sum(s.size() for s in log.segments)
        i = 0
        while i < len(log.segments) - 1 and total > limit:
            seg = log.segments[i]
            total -= seg.size()
            try:
                seg.log_path.unlink()
                seg.index_path.unlink()
            except OSError:
                pass
            i += 1
        if i:
            log.segments = log.segments[i:]
            log.log_start_offset = log.segments[0].base_offset

    def _rewrite_log(self, log, survivors):
        for s in log.segments:
            try:
                s.log_path.unlink(); s.index_path.unlink()
            except OSError:
                pass
        base = log.segments[0].base_offset
        seg = Segment(log.dir, base)
        for off, key, value in survivors:
            batch = build_record_batch([(key, value)], base_offset=off)
            seg.append_batch(batch, off)
        log.segments = [seg]
        # log_end_offset is unchanged (compaction does not change the LEO)

    @staticmethod
    def _w_array(w, items, flexible, writer_fn):
        if flexible:
            w.unsigned_varint(len(items) + 1)
        else:
            w.int32(len(items))
        for it in items:
            writer_fn(w, it)



def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--node-id", type=int, default=1)
    args = ap.parse_args(argv)
    broker = Broker(args.host, args.port, Path(args.data_dir), args.node_id)
    try:
        asyncio.run(broker.serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
