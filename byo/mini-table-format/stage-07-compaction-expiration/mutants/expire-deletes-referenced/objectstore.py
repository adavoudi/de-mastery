"""A tiny object store over a local directory — PROVIDED, do not edit.

This models the one primitive a real object store (S3 with conditional writes,
GCS, Azure) gives you for building a table format: an *atomic, conditional*
"create this object only if it does not already exist" (compare-and-swap on
non-existence). Everything else — snapshots, commits, concurrency control — you
build on top of this in ``tableformat.py``.

``put_if_absent`` is the whole point of this file. It is implemented with
``os.open(..., O_CREAT | O_EXCL)``, whose atomicity the OS guarantees even across
processes on a local filesystem (the same guarantee S3 now exposes as
``If-None-Match: *`` / conditional PUT). You implement the *protocol* that uses it;
you never re-implement the primitive.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path


class ObjectStore:
    """Namespaced key/value store backed by files under ``root``.

    Keys are POSIX-style relative paths (e.g. ``"t/metadata/v3.json"``). Values are
    bytes. Keys map 1:1 to files, so a real Parquet/JSON reader can open them.
    """

    def __init__(self, root: str | os.PathLike):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # --- path helpers -----------------------------------------------------
    def path(self, key: str) -> Path:
        """Absolute filesystem path for ``key`` (so you can hand it to pyarrow)."""
        return self.root / key

    # --- reads ------------------------------------------------------------
    def exists(self, key: str) -> bool:
        return self.path(key).exists()

    def get(self, key: str) -> bytes:
        return self.path(key).read_bytes()

    def list(self, prefix: str = "") -> list[str]:
        """All keys under ``prefix`` (sorted)."""
        base = self.root
        out: list[str] = []
        for p in base.rglob("*"):
            if p.is_file() and ".tmp." not in p.name:
                rel = p.relative_to(base).as_posix()
                if rel.startswith(prefix):
                    out.append(rel)
        return sorted(out)

    # --- unconditional write (for data files, which get unique names) -----
    def put(self, key: str, data: bytes) -> None:
        p = self.path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + f".tmp.{os.getpid()}.{id(data)}")
        tmp.write_bytes(data)
        os.replace(tmp, p)  # atomic rename over any existing file

    # --- the one conditional primitive ------------------------------------
    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Atomically create ``key`` with ``data`` IFF it does not already exist.

        Returns True if THIS caller created it, False if it already existed. This
        is the only compare-and-set the store offers; build your commit protocol
        on it.

        The destination only becomes visible once it already holds the complete
        value (no torn reads): we write a unique temp file in full, then
        ``os.link`` it into place. ``link`` is atomic and raises ``FileExistsError``
        if the target already exists — exactly the conditional-create guarantee S3
        exposes as ``If-None-Match: *``. (On a local FS this is stronger than a bare
        ``O_CREAT|O_EXCL`` open, whose empty file is observable before the write.)
        """
        p = self.path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f".{p.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
        fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            try:
                os.link(tmp, p)      # atomic; fails if p already exists
            except FileExistsError:
                return False
            return True
        finally:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass

    def delete(self, key: str) -> None:
        p = self.path(key)
        if p.exists():
            p.unlink()
