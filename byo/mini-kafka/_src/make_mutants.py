#!/usr/bin/env python3
"""Generate >=3 mutants per stage for KFK-BYO.

Each mutant is the full cumulative reference (broker_full.py) with ONE
string-patched defect that breaks stage N while leaving stages < N correct, plus
a one-line WHY.md.  Run from byo/mini-kafka/_src/.
"""
from pathlib import Path

HERE = Path(__file__).parent
CH = HERE.parent
FULL = (HERE / "broker_full.py").read_text()

STAGE_DIR = {
    1: "stage-01-apiversions-tcp",
    2: "stage-02-segmented-log-index",
    3: "stage-03-produce-recordbatch-crc",
    4: "stage-04-fetch-by-offset",
    5: "stage-05-metadata-topics-partitions",
    6: "stage-06-retention-compaction",
    7: "stage-07-consumer-group-offsets",
    8: "stage-08-replication-high-watermark",
}


def patch(old, new):
    assert old in FULL, f"anchor not found:\n{old[:120]}"
    return FULL.replace(old, new, 1)


# Each entry: stage -> list of (name, why, mutated_source)
MUTANTS = {}


def add(stage, name, why, src):
    MUTANTS.setdefault(stage, []).append((name, why, src))


# ---- Stage 1 --------------------------------------------------------------
add(1, "error-code-nonzero",
    "ApiVersions returns error_code=1, so clients treat the broker as unusable.",
    patch("        w.int16(NO_ERROR)\n        keys = sorted(SUPPORTED.items())",
          "        w.int16(1)\n        keys = sorted(SUPPORTED.items())"))

add(1, "omit-produce-api",
    "Does not advertise Produce(0), so the api table is incomplete and the stage-1 contract fails.",
    patch("SUPPORTED = {\n    PRODUCE: (0, 11),\n",
          "SUPPORTED = {\n"))

add(1, "flex-response-header",
    "Adds a tagged-field section to the ApiVersions response header; the header must be v0, so the decode is off by one byte.",
    patch("        w = self._resp_header(corr_id, flexible, api_versions_quirk=True)\n        w.int16(NO_ERROR)\n        keys = sorted(SUPPORTED.items())",
          "        w = self._resp_header(corr_id, flexible, api_versions_quirk=False)\n        w.int16(NO_ERROR)\n        keys = sorted(SUPPORTED.items())"))


# ---- Stage 2 (keep produce working, break the index) ----------------------
add(2, "no-index-write",
    "Never writes offset-index entries, so the .index file stays empty.",
    patch("        if self._bytes_since_index >= INDEX_INTERVAL_BYTES or pos == 0:\n            rel = base_offset - self.base_offset\n            with open(self.index_path, \"ab\") as idx:\n                idx.write(struct.pack(\">ii\", rel, pos))\n            self._bytes_since_index = 0",
          "        if False:\n            rel = base_offset - self.base_offset\n            with open(self.index_path, \"ab\") as idx:\n                idx.write(struct.pack(\">ii\", rel, pos))\n            self._bytes_since_index = 0"))

add(2, "index-position-always-zero",
    "Writes every index byte-position as 0, so the index no longer points into the log.",
    patch("            with open(self.index_path, \"ab\") as idx:\n                idx.write(struct.pack(\">ii\", rel, pos))",
          "            with open(self.index_path, \"ab\") as idx:\n                idx.write(struct.pack(\">ii\", rel, 0 if pos else pos))\n                idx.seek(0); idx.truncate()\n                idx.write(struct.pack(\">ii\", rel, 999999))"))

add(2, "segment-not-named-by-base",
    "Names the first segment '1.log' instead of base offset 0, breaking segment discovery.",
    patch("        self.log_path = directory / f\"{base_offset:020d}.log\"\n        self.index_path = directory / f\"{base_offset:020d}.index\"",
          "        self.log_path = directory / f\"{base_offset + 1:020d}.log\"\n        self.index_path = directory / f\"{base_offset + 1:020d}.index\""))


# ---- Stage 3 (keep storage, break produce/crc/offset) ---------------------
add(3, "corrupt-crc",
    "Stores the batch with crc field zeroed, so the persisted RecordBatch has an invalid CRC32C.",
    patch("        base = log.log_end_offset\n        for batch_bytes, _last, _base in iter_batches_with_bytes(records):\n            log.append(batch_bytes, record_count(batch_bytes))\n        self._apply_cleanup(topic, pi)",
          "        base = log.log_end_offset\n        for batch_bytes, _last, _base in iter_batches_with_bytes(records):\n            bb = bytearray(batch_bytes); struct.pack_into('>I', bb, 17, 0)\n            log.append(bytes(bb), record_count(batch_bytes))\n        self._apply_cleanup(topic, pi)"))

add(3, "no-base-offset-stamp",
    "Does not stamp the assigned base offset into the stored batch, so on-disk base offsets stay 0.",
    patch("    def append(self, raw_batch, count):\n        base = self.log_end_offset\n        stamped = stamp_base_offset(raw_batch, base)",
          "    def append(self, raw_batch, count):\n        base = self.log_end_offset\n        stamped = raw_batch"))

add(3, "offset-off-by-one",
    "Advances the log-end offset by count+1, so produced offsets are not contiguous.",
    patch("        self.log_end_offset = base + count\n        # single-node",
          "        self.log_end_offset = base + count + 1\n        # single-node"))


# ---- Stage 4 (keep produce, break fetch/listoffsets) ----------------------
add(4, "fetch-ignores-offset",
    "Fetch always serves from offset 0, ignoring fetch_offset, so mid-log reads return the whole log.",
    patch("        records = log.read(fetch_offset, max_bytes) if log else b\"\"",
          "        records = log.read(0, max_bytes) if log else b\"\""))

add(4, "listoffsets-latest-is-earliest",
    "ListOffsets returns the earliest offset for a 'latest' query, so end offsets read as 0.",
    patch("        offset = earliest if ts == -2 else latest",
          "        offset = earliest"))

add(4, "fetch-drops-last-record",
    "Fetch stops one record short of the end, so the last produced record is never delivered.",
    patch("        if fetch_offset >= self.high_watermark:\n            return b\"\"\n        seg = self.segments[0]",
          "        if fetch_offset >= self.high_watermark - 1:\n            return b\"\"\n        seg = self.segments[0]"))


# ---- Stage 5 (keep single-topic/partition, break multi) -------------------
add(5, "metadata-always-one-partition",
    "Metadata always reports a single partition regardless of the real count.",
    patch("        nparts = self.topics.get(topic, 0)\n        self._w_array(w, list(range(nparts)), flexible,",
          "        nparts = min(1, self.topics.get(topic, 0))\n        self._w_array(w, list(range(nparts)), flexible,"))

add(5, "produce-ignores-partition",
    "All produces land in partition 0, so records written to other partitions are lost/misplaced.",
    patch("                pi = r.int32()\n                records = r.compact_bytes() if flexible else r.bytes_()",
          "                pi = r.int32(); pi = 0\n                records = r.compact_bytes() if flexible else r.bytes_()"))

add(5, "shared-log-across-topics",
    "Logs are keyed by partition only, ignoring the topic name, so two topics share one log.",
    patch("        key = (topic, partition)\n        if key not in self.logs:\n            d = self.data_dir / topic / f\"partition-{partition}\"",
          "        key = (\"_shared\", partition)\n        if key not in self.logs:\n            d = self.data_dir / \"_shared\" / f\"partition-{partition}\""))


# ---- Stage 6 (keep produce/fetch, break compaction/retention) -------------
add(6, "no-compaction",
    "Ignores cleanup.policy=compact, so every version of a key survives.",
    patch("        if \"compact\" in policy:\n            self._compact(log)",
          "        if False and \"compact\" in policy:\n            self._compact(log)"))

add(6, "compact-keeps-first",
    "Compaction keeps the FIRST value per key instead of the latest.",
    patch("                if key not in latest:\n                    order.append(key)\n                latest[key] = (value, off)",
          "                if key not in latest:\n                    order.append(key)\n                    latest[key] = (value, off)"))

add(6, "compact-shrinks-leo",
    "Compaction resets the log-end offset to the survivor count, corrupting future offsets.",
    patch("        log.segments = [seg]\n        # log_end_offset is unchanged (compaction does not change the LEO)",
          "        log.segments = [seg]\n        log.log_end_offset = len(survivors)\n        log.high_watermark = len(survivors)"))


# ---- Stage 7 (keep everything else, break group offsets) ------------------
add(7, "commit-noop",
    "OffsetCommit does not persist the offset, so a later OffsetFetch finds nothing.",
    patch("                self.group_offsets[(group, topic, pi)] = committed\n                parts.append(pi)",
          "                parts.append(pi)"))

add(7, "offsets-ignore-group",
    "Committed offsets are keyed without the group, so two groups clobber each other.",
    patch("                self.group_offsets[(group, topic, pi)] = committed\n                parts.append(pi)",
          "                self.group_offsets[(\"_g\", topic, pi)] = committed\n                parts.append(pi)"))

add(7, "offsetfetch-returns-leo",
    "OffsetFetch returns the log-end offset instead of the committed offset, so it never reads -1 for an uncommitted group.",
    patch("    def _w_offfetch_part(self, w, group, topic, pi, ver, flexible):\n        off = self.group_offsets.get((group, topic, pi), -1)",
          "    def _w_offfetch_part(self, w, group, topic, pi, ver, flexible):\n        _log = self.logs.get((topic, pi))\n        off = _log.log_end_offset if _log else -1"))


# ---- Stage 8 (keep everything else, break HW) -----------------------------
add(8, "hw-stuck-at-zero",
    "The high watermark never advances past 0, so consumers see no committed records' end.",
    patch("        self._fsync_active()\n        self.high_watermark = self.log_end_offset\n        return base",
          "        self._fsync_active()\n        self.high_watermark = 0\n        return base"))

add(8, "hw-not-restored",
    "On startup the HW is reset to 0 instead of the durable log end, so it is lost across a restart.",
    patch("        self.log_end_offset = leo\n        self.high_watermark = leo",
          "        self.log_end_offset = leo\n        self.high_watermark = 0"))

add(8, "hw-lags-by-one",
    "The high watermark is set to log-end minus one, so the newest record is never visible to consumers and the end offset reads one short.",
    patch("        self._fsync_active()\n        self.high_watermark = self.log_end_offset\n        return base",
          "        self._fsync_active()\n        self.high_watermark = max(0, self.log_end_offset - 1)\n        return base"))


def main():
    for stage, muts in MUTANTS.items():
        base = CH / STAGE_DIR[stage] / "mutants"
        for name, why, src in muts:
            d = base / name
            d.mkdir(parents=True, exist_ok=True)
            (d / "broker.py").write_text(src)
            (d / "WHY.md").write_text(why + "\n")
            print(f"stage {stage}: {name}")


if __name__ == "__main__":
    main()
