"""Black-box harness for the mini-Kafka BYO challenge (shipped into every stage's
tests/ dir). It launches the learner's ``broker.py`` as a subprocess on a random
free port with a per-test data dir, waits for the ``LISTENING`` line (no sleep),
and gives tests two ways to talk to it:

  * raw sockets (``send_request`` / request+response framing + a tiny protocol
    encoder) for stage 1-2 byte-level checks, and
  * real ``confluent-kafka`` (librdkafka) clients for produce/fetch/offsets.

Tests NEVER import the learner's module; they only speak the wire protocol, so
the learner may implement the broker any way they like.
"""

from __future__ import annotations

import os
import re
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def submission_dir() -> Path:
    d = os.environ.get("DE_SUBMISSION_DIR")
    assert d, "DE_SUBMISSION_DIR not set"
    return Path(d)


class MiniBroker:
    """A running instance of the learner's broker.py."""

    def __init__(self, data_dir: Path, node_id: int = 1):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.node_id = node_id
        self.proc: subprocess.Popen | None = None
        self.host = "127.0.0.1"
        self.port: int | None = None
        self._log = self.data_dir / "broker.out"

    def start(self, timeout: float = 20.0) -> "MiniBroker":
        entry = os.environ.get("DE_SUBMISSION_ENTRY", "broker.py")
        script = submission_dir() / entry
        assert script.exists(), f"broker entry not found: {script}"
        logf = open(self._log, "w")
        self.proc = subprocess.Popen(
            [sys.executable, str(script), "--data-dir", str(self.data_dir / "data"),
             "--host", self.host, "--port", "0", "--node-id", str(self.node_id)],
            stdout=subprocess.PIPE, stderr=logf, text=True, start_new_session=True)
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                if self.proc.poll() is not None:
                    raise RuntimeError(
                        f"broker exited early (rc={self.proc.returncode}):\n{self._tail()}")
                continue
            m = re.match(r"LISTENING (\S+):(\d+)", line.strip())
            if m:
                self.host, self.port = m.group(1), int(m.group(2))
                return self
        raise TimeoutError(f"broker never printed LISTENING within {timeout}s\n{self._tail()}")

    @property
    def bootstrap(self) -> str:
        return f"{self.host}:{self.port}"

    def _tail(self, n: int = 40) -> str:
        try:
            return "\n".join(self._log.read_text(errors="replace").splitlines()[-n:])
        except OSError:
            return "(no log)"

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
                self.proc.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if self.proc and self.proc.stdout:
            try:
                self.proc.stdout.close()
            except Exception:
                pass

    # -- raw socket round trip -------------------------------------------
    def request(self, body: bytes, timeout: float = 10.0) -> bytes:
        """Send a length-framed request body, return the response body
        (length prefix stripped)."""
        with socket.create_connection((self.host, self.port), timeout=timeout) as s:
            s.sendall(struct.pack(">i", len(body)) + body)
            size = _recv_exactly(s, 4)
            (n,) = struct.unpack(">i", size)
            return _recv_exactly(s, n)


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed mid-response")
        buf += chunk
    return buf


# ---------------------------------------------------------------------------
# Minimal request encoders / response decoders for raw-socket tests
# ---------------------------------------------------------------------------
def _put_string(buf: bytearray, s: str | None):
    if s is None:
        buf += struct.pack(">h", -1)
    else:
        e = s.encode(); buf += struct.pack(">h", len(e)) + e


def _put_uvarint(buf: bytearray, v: int):
    while True:
        if (v & ~0x7F) == 0:
            buf.append(v); return
        buf.append((v & 0x7F) | 0x80); v >>= 7


def request_header(api_key: int, api_version: int, corr_id: int,
                   client_id: str | None = "mini-test", flexible: bool = False) -> bytes:
    """Build a request header. For flexible APIs set flexible=True to append the
    empty tagged-field section (header v2)."""
    h = bytearray()
    h += struct.pack(">hhi", api_key, api_version, corr_id)
    _put_string(h, client_id)
    if flexible:
        _put_uvarint(h, 0)   # empty tagged fields
    return bytes(h)


def api_versions_request(corr_id: int = 1, api_version: int = 3) -> bytes:
    """An ApiVersions request. v3 is flexible, with two compact-string fields
    (client software name/version) in the body."""
    flexible = api_version >= 3
    body = bytearray(request_header(18, api_version, corr_id, flexible=flexible))
    if flexible:
        # ClientSoftwareName + ClientSoftwareVersion (compact strings) + tags
        for s in ("mini-test", "1.0"):
            e = s.encode(); _put_uvarint(body, len(e) + 1); body += e
        _put_uvarint(body, 0)
    return bytes(body)


class RespReader:
    """Just enough response decoding for the raw-socket stage-1 test."""

    def __init__(self, b: bytes):
        self.b = b; self.i = 0

    def int16(self):
        v = struct.unpack_from(">h", self.b, self.i)[0]; self.i += 2; return v

    def int32(self):
        v = struct.unpack_from(">i", self.b, self.i)[0]; self.i += 4; return v

    def uvarint(self):
        res = 0; shift = 0
        while True:
            byte = self.b[self.i]; self.i += 1
            res |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return res
            shift += 7


def parse_api_versions_response(resp: bytes, flexible: bool = True) -> dict:
    """Return {api_key: (min, max)} from an ApiVersions response. The response
    header for ApiVersions is ALWAYS v0 (no tagged fields), even in v3."""
    r = RespReader(resp)
    r.int32()                 # correlation id (header v0)
    error = r.int16()
    out = {"error": error, "apis": {}}
    if flexible:
        n = r.uvarint() - 1
        for _ in range(n):
            k = r.int16(); lo = r.int16(); hi = r.int16(); r.uvarint()
            out["apis"][k] = (lo, hi)
    else:
        n = r.int32()
        for _ in range(n):
            k = r.int16(); lo = r.int16(); hi = r.int16()
            out["apis"][k] = (lo, hi)
    return out


# ---------------------------------------------------------------------------
# confluent-kafka helpers (produce / consume with manual assignment)
# ---------------------------------------------------------------------------
def new_producer(bootstrap: str, **extra):
    from confluent_kafka import Producer
    conf = {"bootstrap.servers": bootstrap,
            "enable.idempotence": False,
            "retries": 2,
            "message.timeout.ms": 6000,
            "socket.timeout.ms": 4000}
    conf.update(extra)
    return Producer(conf)


def produce_sync(bootstrap: str, topic: str, records, partition=None, **extra):
    """records: list of (key, value) as bytes/None. Returns list of offsets."""
    p = new_producer(bootstrap, **extra)
    offsets = []
    errs = []

    def cb(err, msg):
        if err:
            errs.append(err)
        else:
            offsets.append(msg.offset())

    for k, v in records:
        kw = {"key": k, "value": v, "on_delivery": cb}
        if partition is not None:
            kw["partition"] = partition
        p.produce(topic, **kw)
    p.flush(8)
    assert not errs, f"produce errors: {errs}"
    return offsets


def produce_separate_batches(bootstrap: str, topic: str, records, partition=None):
    """Produce each record and flush, so each lands in its OWN RecordBatch.
    Returns the list of offsets in order."""
    p = new_producer(bootstrap)
    offsets = []
    errs = []

    def cb(err, msg):
        (errs if err else offsets).append(err or msg.offset())

    for k, v in records:
        kw = {"key": k, "value": v, "on_delivery": cb}
        if partition is not None:
            kw["partition"] = partition
        p.produce(topic, **kw)
        p.flush(8)
    assert not errs, f"produce errors: {errs}"
    return offsets


def consume_from(bootstrap: str, topic: str, partition: int, start_offset: int,
                 count: int, timeout: float = 6.0, group: str | None = None):
    """Manual-assignment consume (no group protocol). Returns list of
    (key, value, offset)."""
    from confluent_kafka import Consumer, TopicPartition
    import uuid
    conf = {"bootstrap.servers": bootstrap,
            "group.id": group or ("g-" + uuid.uuid4().hex[:8]),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest"}
    c = Consumer(conf)
    c.assign([TopicPartition(topic, partition, start_offset)])
    out = []
    deadline = time.time() + timeout
    try:
        while time.time() < deadline and len(out) < count:
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue
            out.append((msg.key(), msg.value(), msg.offset()))
    finally:
        c.close()
    return out


def raw_fetch_first_base_offset(broker, topic, partition, fetch_offset):
    """Send a raw Fetch v12 request for one partition and return the base offset
    of the FIRST record batch in the response (or None if empty). Lets a test
    assert the broker honours fetch_offset byte-for-byte."""
    ver = 12
    body = bytearray(request_header(1, ver, 55, flexible=True))
    body += struct.pack(">i", -1)      # replica_id (v<15)
    body += struct.pack(">i", 500)     # max_wait_ms
    body += struct.pack(">i", 1)       # min_bytes
    body += struct.pack(">i", 1 << 20)  # max_bytes
    body += struct.pack(">b", 0)       # isolation_level
    body += struct.pack(">i", 0)       # session_id
    body += struct.pack(">i", -1)      # session_epoch
    # topics: compact array of 1
    _put_uvarint(body, 2)              # 1 topic + 1
    _put_uvarint(body, len(topic.encode()) + 1); body += topic.encode()
    _put_uvarint(body, 2)              # 1 partition + 1
    body += struct.pack(">i", partition)
    body += struct.pack(">i", -1)      # current_leader_epoch
    body += struct.pack(">q", fetch_offset)
    body += struct.pack(">i", -1)      # last_fetched_epoch
    body += struct.pack(">q", -1)      # log_start_offset
    body += struct.pack(">i", 1 << 20)  # partition_max_bytes
    _put_uvarint(body, 0)              # partition tagged fields
    _put_uvarint(body, 0)              # topic tagged fields
    _put_uvarint(body, 0)              # forgotten topics (compact array = empty)
    _put_uvarint(body, 1)              # rack_id compact string "" -> len0+1
    _put_uvarint(body, 0)              # request tagged fields
    resp = broker.request(bytes(body))
    return _first_batch_base(resp)


def _first_batch_base(resp: bytes):
    """Find the first v2 RecordBatch (magic byte == 2 at offset+16) in a fetch
    response and return its base offset."""
    i = 0
    n = len(resp)
    while i + 61 <= n:
        # a v2 batch has magic==2 at byte 16 of the batch; scan for the pattern
        # baseOffset(8) batchLength(4) partitionLeaderEpoch(4) magic(1)==2
        if resp[i + 16] == 2:
            batch_len = struct.unpack_from(">i", resp, i + 8)[0]
            if 0 < batch_len < n:
                return struct.unpack_from(">q", resp, i)[0]
        i += 1
    return None
