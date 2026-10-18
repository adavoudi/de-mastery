"""Schema compatibility helpers + native Schema Registry support (KFK-06).

Two layers, both NO-DOCKER-first:

1. ``check_compatibility(reader, writer, mode)`` — a pure-Python Avro
   compatibility checker built on fastavro schema resolution. This is the
   **primary graded path** for KFK-06: it needs no Schema Registry process at
   all, so a schema-evolution exercise is always gradable on a bare machine.
   It mirrors Confluent's compatibility semantics for the common cases:
     * BACKWARD  — a reader using the *new* schema can read data written with
       the *old* schema.
     * FORWARD   — a reader using the *old* schema can read data written with
       the *new* schema.
     * FULL      — both directions.
     * NONE      — always compatible.
   Compatibility is decided by actually round-tripping representative records
   through ``schemaless_writer`` / ``schemaless_reader`` and catching
   ``SchemaResolutionError`` — i.e. the same mechanism a consumer would hit at
   runtime, not a hand-coded rule table.

2. ``start_schema_registry(bootstrap)`` + the ``schema_registry`` fixture — if
   a native Confluent Community Schema Registry has been vendored
   (``labs/fetch_schema_registry.sh`` into ``labs/.schema-registry``), start it
   on the vendored JDK against the given broker and return its URL. The fixture
   SKIPS cleanly when the vendored SR is absent AND the ``streaming`` lab is
   down. Exercises use the registry as an *extra* layer; grading never requires
   it (layer 1 is the graded path).
"""

from __future__ import annotations

import io
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from de_testkit.java_env import repo_root, resolve as resolve_java


# ---------------------------------------------------------------------------
# layer 1 — pure-Python Avro compatibility (no process)
# ---------------------------------------------------------------------------
def _sample_records(schema: dict) -> list[dict]:
    """Build a few representative records that satisfy ``schema`` using field
    defaults where present and type-appropriate placeholders otherwise."""
    import fastavro  # noqa: F401

    def val_for(ftype):
        if isinstance(ftype, list):  # union
            # prefer a non-null branch for coverage
            non_null = [t for t in ftype if t != "null"]
            return val_for(non_null[0]) if non_null else None
        if isinstance(ftype, dict):
            t = ftype.get("type")
            if t == "array":
                return []
            if t == "map":
                return {}
            if t == "record":
                return {f["name"]: val_for(f.get("type")) for f in ftype["fields"]}
            if t == "enum":
                return ftype["symbols"][0]
            return val_for(t)
        return {
            "string": "x", "bytes": b"x", "int": 1, "long": 1,
            "float": 1.0, "double": 1.0, "boolean": True, "null": None,
        }.get(ftype, None)

    rec = {}
    for f in schema["fields"]:
        if "default" in f:
            rec[f["name"]] = f["default"]
        else:
            rec[f["name"]] = val_for(f["type"])
    return [rec]


def _can_read(writer: dict, reader: dict) -> bool:
    """True if a reader with ``reader`` schema can read data written with
    ``writer`` schema (Avro schema resolution succeeds for sample records)."""
    import fastavro
    from fastavro import schemaless_reader, schemaless_writer
    from fastavro.schema import parse_schema

    pw = parse_schema(dict(writer))
    pr = parse_schema(dict(reader))
    for rec in _sample_records(writer):
        buf = io.BytesIO()
        schemaless_writer(buf, pw, rec)
        buf.seek(0)
        try:
            schemaless_reader(buf, pw, pr)
        except Exception:  # fastavro.read.SchemaResolutionError & friends
            return False
    return True


def check_compatibility(reader_schema: dict, writer_schema: dict,
                        mode: str = "BACKWARD") -> bool:
    """Is evolving from ``writer_schema`` (old/registered) to ``reader_schema``
    (new/proposed) compatible under ``mode``?

    Confluent convention: for BACKWARD, the NEW schema (consumer) must read data
    produced with the OLD schema. ``reader_schema`` is the new one here.
    """
    mode = mode.upper()
    old, new = writer_schema, reader_schema
    if mode == "NONE":
        return True
    if mode == "BACKWARD":
        return _can_read(writer=old, reader=new)
    if mode == "FORWARD":
        return _can_read(writer=new, reader=old)
    if mode == "FULL":
        return _can_read(writer=old, reader=new) and _can_read(writer=new, reader=old)
    raise ValueError(f"unknown compatibility mode: {mode}")


# ---------------------------------------------------------------------------
# layer 2 — native Confluent Community Schema Registry (optional)
# ---------------------------------------------------------------------------
def schema_registry_home(root: Path | None = None) -> Path | None:
    """``labs/.schema-registry/<dir>`` if it has bin/schema-registry-start."""
    base = (root or repo_root()) / "labs" / ".schema-registry"
    if not base.is_dir():
        return None
    for child in sorted([base, *base.iterdir()]):
        if child.is_dir() and (child / "bin" / "schema-registry-start").exists():
            return child
    return None


def native_sr_available(root: Path | None = None) -> tuple[bool, str]:
    if schema_registry_home(root) is None:
        return False, (
            "native Schema Registry not vendored. Run "
            "`bash labs/fetch_schema_registry.sh` (Confluent Community tarball), "
            "or use the fastavro-based de_testkit.check_compatibility fallback, "
            "or bring up the `streaming` lab.")
    info = resolve_java()
    if not info["usable"]:
        return False, f"Java unusable: {info['reason']}"
    return True, ""


def _free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


@dataclass
class SchemaRegistry:
    url: str
    popen: subprocess.Popen
    work: Path

    def client(self):
        from confluent_kafka.schema_registry import SchemaRegistryClient
        return SchemaRegistryClient({"url": self.url})

    def stop(self) -> None:
        import os
        import shutil
        import signal
        if self.popen.poll() is None:
            try:
                os.killpg(os.getpgid(self.popen.pid), signal.SIGTERM)
                self.popen.wait(timeout=15)
            except Exception:  # noqa: BLE001
                try:
                    os.killpg(os.getpgid(self.popen.pid), signal.SIGKILL)
                except Exception:  # noqa: BLE001
                    pass
        shutil.rmtree(self.work, ignore_errors=True)


def start_schema_registry(bootstrap_servers: str,
                          root: Path | None = None) -> SchemaRegistry:
    """Start the vendored Confluent Community Schema Registry against the given
    brokers, on a free port, using the vendored JDK. Deterministic readiness
    poll on the ``/subjects`` endpoint. Raises if the vendored SR is absent."""
    import os

    home = schema_registry_home(root)
    if home is None:
        raise RuntimeError("native Schema Registry not vendored "
                           "(run labs/fetch_schema_registry.sh)")
    info = resolve_java()
    work = Path(tempfile.mkdtemp(prefix="sr-"))
    port = _free_port()
    props = work / "schema-registry.properties"
    props.write_text(
        f"listeners=http://127.0.0.1:{port}\n"
        f"kafkastore.bootstrap.servers=PLAINTEXT://{bootstrap_servers}\n"
        "kafkastore.topic=_schemas\n"
        "schema.compatibility.level=backward\n")
    env = dict(os.environ)
    if info["usable"]:
        env["JAVA_HOME"] = info["java_home"]
        env["PATH"] = str(Path(info["java_home"]) / "bin") + os.pathsep + env.get("PATH", "")
    env["SCHEMA_REGISTRY_HEAP_OPTS"] = "-Xmx512m"
    env["LOG_DIR"] = str(work / "log")
    log = work / "sr.out"
    popen = subprocess.Popen(
        [str(home / "bin" / "schema-registry-start"), str(props)],
        env=env, stdout=open(log, "w"), stderr=subprocess.STDOUT,
        start_new_session=True)
    sr = SchemaRegistry(url=f"http://127.0.0.1:{port}", popen=popen, work=work)

    import urllib.request
    deadline = time.time() + 60
    while time.time() < deadline:
        if popen.poll() is not None:
            tail = "\n".join(log.read_text(errors="replace").splitlines()[-40:])
            raise RuntimeError(f"schema-registry died:\n{tail}")
        try:
            with urllib.request.urlopen(f"{sr.url}/subjects", timeout=3):
                return sr
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
    sr.stop()
    raise TimeoutError("schema-registry not ready within 60s")
