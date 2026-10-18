"""Deterministic Structured Streaming test helpers.

PySpark (unlike Scala Spark) exposes no ``MemoryStream``. The reliable, flake-free
way to drive a streaming query in a test is:

1. write input *batch files* (JSON or Parquet) into a source directory,
2. start a streaming read over that directory with a known schema,
3. advance the query one micro-batch at a time with
   ``trigger(availableNow=True)`` + ``processAllAvailable()`` so there is no
   wall-clock ``sleep()`` and the run is fully deterministic.

This matches docs/TESTING.md: controlled time, no ``sleep()``, and the final
materialized state must equal the batch answer (DuckDB oracle).

Example (SPK-10/11 author)::

    def test_stream_dedup(spark_iceberg, tmp_path):
        src = FileStreamSource(spark_iceberg, tmp_path, fmt="json",
                               schema="id INT, ts TIMESTAMP, amt DOUBLE")
        src.add([{"id": 1, "ts": "2026-01-01T00:00:00", "amt": 10.0}])
        reader = src.read_stream()                 # a streaming DataFrame
        q = (reader.writeStream.format("memory").queryName("out")
             .outputMode("append").trigger(availableNow=True).start())
        q.processAllAvailable(); q.stop()
        rows = spark_iceberg.sql("SELECT * FROM out").collect()
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence


class FileStreamSource:
    """A directory-backed streaming source you append micro-batches to."""

    def __init__(self, spark, base_dir, *, fmt: str = "json",
                 schema: str | Any | None = None, name: str = "src"):
        self.spark = spark
        self.dir = Path(base_dir) / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.fmt = fmt
        self.schema = schema
        self._batch = 0

    def add(self, rows: Sequence[dict]) -> Path:
        """Write one micro-batch file. Returns the file path."""
        self._batch += 1
        if self.fmt == "json":
            path = self.dir / f"batch-{self._batch:04d}.json"
            with path.open("w") as fh:
                for r in rows:
                    fh.write(json.dumps(r, default=str) + "\n")
            return path
        if self.fmt == "parquet":
            # Write via Spark so types match the streaming reader exactly.
            sub = self.dir / f"batch-{self._batch:04d}"
            df = self.spark.createDataFrame(list(rows))
            df.write.mode("overwrite").parquet(str(sub))
            return sub
        raise ValueError(f"unsupported fmt: {self.fmt!r}")

    def read_stream(self):
        """Return a streaming DataFrame reading this source directory."""
        reader = self.spark.readStream.format(self.fmt)
        if self.schema is not None:
            reader = reader.schema(self.schema)
        if self.fmt == "json":
            reader = reader.option("multiLine", "false")
        return reader.load(str(self.dir))


def drive_available(df, *, sink: str = "memory", query_name: str = "out",
                    output_mode: str = "append", checkpoint: str | None = None,
                    extra_options: dict | None = None, path: str | None = None):
    """Run a streaming DataFrame to completion with ``availableNow`` and return
    the (stopped) ``StreamingQuery``.

    No ``sleep()``: ``trigger(availableNow=True)`` + ``processAllAvailable()``
    processes every file currently present, then stops.
    """
    writer = df.writeStream.outputMode(output_mode).trigger(availableNow=True)
    if sink == "memory":
        writer = writer.format("memory").queryName(query_name)
    else:
        writer = writer.format(sink)
        if path is not None:
            writer = writer.option("path", path)
    if checkpoint is not None:
        writer = writer.option("checkpointLocation", checkpoint)
    for k, v in (extra_options or {}).items():
        writer = writer.option(k, v)
    query = writer.start()
    query.processAllAvailable()
    query.stop()
    return query
