#!/usr/bin/env python3
"""Build the KFK-BYO stage tree from _src/. Run from byo/mini-kafka/_src/.

Distributes:
  * stage.yaml (title, entry: broker.py, timeout_s, 3 hints) per stage,
  * README.md per stage,
  * tests/ : conftest.py (from conftest_template.py), _broker_harness.py, and
    test_stageN.py,
  * solution/broker.py  (= broker_full.py, cumulative for every stage),
  * alt_solutions/threaded/broker.py (= broker_alt.py).

Mutants are produced by make_mutants.py.
"""
import shutil
from pathlib import Path

HERE = Path(__file__).parent
CH = HERE.parent

STAGES = [
    (1, "apiversions-tcp"),
    (2, "segmented-log-index"),
    (3, "produce-recordbatch-crc"),
    (4, "fetch-by-offset"),
    (5, "metadata-topics-partitions"),
    (6, "retention-compaction"),
    (7, "consumer-group-offsets"),
    (8, "replication-high-watermark"),
]

TITLES = {
    1: "ApiVersions over TCP",
    2: "A segmented log with an offset index",
    3: "Produce: RecordBatch v2 + CRC32C",
    4: "Fetch by offset",
    5: "Metadata + multiple topics & partitions",
    6: "Retention and log compaction",
    7: "Consumer-group offsets (OffsetCommit / OffsetFetch)",
    8: "Replication & the high watermark",
}

TIMEOUT = {1: 90, 2: 90, 3: 90, 4: 120, 5: 120, 6: 120, 7: 120, 8: 120}

HINTS = {
    1: [
        "A request starts with a 4-byte length, then the header: api_key int16, api_version int16, correlation_id int32, client_id (nullable string, 2-byte length). ApiVersions is api_key 18.",
        "The ApiVersions RESPONSE header is just the correlation_id (int32) with NO tagged-field section, even for v3 — this is a well-known quirk. The body is: error_code int16, then a COMPACT array of {api_key int16, min int16, max int16, tagged_fields} for v3, then throttle_time_ms int32 and tagged_fields.",
        "Advertise at least ApiVersions(18), Metadata(3), Produce(0) and Fetch(1) with version ranges your later stages will implement. kafka-broker-api-versions.sh prints the table it parses, so start there.",
    ],
    2: [
        "Give each (topic, partition) its own directory and store records in '<baseOffset>.log' files (20-digit zero-padded). Keep a parallel '<baseOffset>.index' of 8-byte entries: (relative_offset int32, byte_position int32).",
        "Write a sparse index: add an entry for the first batch (offset 0 -> position 0) and then roughly every few KB. The byte position must be where that batch STARTS in the .log file.",
        "Roll to a new segment file (named by the next base offset) once the active one passes a size threshold. On startup, scan existing segments to recompute the log-end offset.",
    ],
    3: [
        "A Produce request (api 0, flexible from v9) carries, per partition, a 'records' blob that is already a serialized RecordBatch v2. Append that blob to the active segment and reply with the base offset you assigned.",
        "RecordBatch v2 layout: baseOffset int64, batchLength int32, partitionLeaderEpoch int32, magic int8 (=2), crc uint32, then attributes/lastOffsetDelta/timestamps/... The CRC is CRC32C (Castagnoli, poly 0x82F63B78) over every byte AFTER the crc field.",
        "Before writing to disk, stamp the batch's baseOffset (the first 8 bytes) with the offset you assigned (the current log-end offset). Advance the log-end offset by the record count (bytes 57..60 of the batch).",
    ],
    4: [
        "A Fetch request (api 1, flexible from v12) asks for a fetch_offset per partition. Return the raw record batches whose records cover offsets >= fetch_offset, as the 'records' bytes field of the response.",
        "Use the offset index to seek near the fetch_offset, then skip whole batches whose last offset (baseOffset + lastOffsetDelta) is below fetch_offset. Respect partition_max_bytes so you don't return the whole log at once.",
        "The response per partition also needs high_watermark and (v4+) last_stable_offset and (v5+) log_start_offset. ListOffsets (api 2) must answer earliest (timestamp -2) = log start and latest (-1) = end so get_watermark_offsets works.",
    ],
    5: [
        "Metadata (api 3) must list every requested topic with its partitions, each partition naming this broker as leader and sole replica/isr. When the client asks for an unknown topic, auto-create it (one partition) so a producer can proceed.",
        "A Produce/Fetch request names a partition index; key your logs by (topic, partition) and create as many partitions as the highest index you see (or the count a CreateTopics request asked for).",
        "Implement CreateTopics (api 19, flexible from v5) so AdminClient.create_topics can set the partition count; store it and report it back in Metadata.",
    ],
    6: [
        "Read the topic's cleanup.policy (set via CreateTopics configs). For 'compact', keep only the LATEST record per key; for 'delete', keep everything (optionally drop old segments by retention.bytes).",
        "Compaction rewrites the log: scan all batches, remember the latest (value, offset) per key, then rebuild one record per surviving key IN OFFSET ORDER, preserving each record's original absolute offset as its batch baseOffset.",
        "Compaction must NOT change the log-end offset (new records still get the next offset). Decode records from a v2 batch by walking the varint-length-prefixed records after the 61-byte header.",
    ],
    7: [
        "FindCoordinator (api 10) must point the client at this broker (it is its own group coordinator). Then OffsetCommit (api 8) stores an offset for (group, topic, partition) and OffsetFetch (api 9) reads it back.",
        "Key your committed offsets by (group, topic, partition). An OffsetFetch for a group/partition with no commit must return -1 (no committed offset).",
        "OffsetFetch v8+ uses a 'groups' array; v9 adds member_id + member_epoch in the request before the topics. Parse those or your topic array will be misaligned. Persist the offsets so a fresh consumer in the same group reads them.",
    ],
    8: [
        "Track a high watermark (HW) per partition: the offset up to which records are committed and visible. On a single node, advance HW to the log-end offset once each record is durably written (flush + fsync).",
        "Serve Fetch and ListOffsets(latest) using the HW, not the raw log-end offset, and never return records at or beyond the HW. A consumer's get_watermark_offsets high value is your HW.",
        "On startup, restore the HW from what is durably on disk (equal to the recomputed log-end offset), so offsets and the HW survive a restart from the same data dir.",
    ],
}

README = {
    1: """# Stage 1 — ApiVersions over TCP

Every Kafka client's first move is an **ApiVersions** request: "which APIs and
versions do you speak?" Your broker must answer it correctly or no real client
will proceed.

The starter already length-frames requests/responses and prints
`LISTENING <host>:<port>`. Implement `dispatch(request_bytes)`:

- parse the request header (api_key, api_version, correlation_id, client_id),
- for ApiVersions (api_key **18**), return a response the official
  `kafka-broker-api-versions.sh` and librdkafka accept.

Advertise at least ApiVersions(18), Metadata(3), Produce(0), Fetch(1) — you will
implement those in later stages, but clients negotiate against this table now.

Tests: a raw-socket decode of your ApiVersions v3 response, and the official CLI
tool connecting to you.
""",
    2: """# Stage 2 — A segmented log with an offset index

Kafka stores each partition as an append-only **log split into segments**, with
a sparse **offset index** mapping offsets to byte positions so reads can seek.

Give every `(topic, partition)` its own directory under the data dir. Store
record batches in `<baseOffset>.log` files and index entries in
`<baseOffset>.index` (8 bytes each: relative-offset int32, byte-position int32).

Tests produce records (via Stage 3's Produce path — the reference implements
both) and then inspect your `.log`/`.index` files directly: a segment named by
base offset 0, a non-empty index whose first entry maps offset 0 to position 0,
and index positions that fall inside the log.
""",
    3: """# Stage 3 — Produce: RecordBatch v2 + CRC32C

Implement **Produce** (api 0). The client sends a serialized **RecordBatch v2**
per partition; you append it to the log and reply with the base offset you
assigned.

- Assign the current log-end offset as the batch's base; stamp it into the
  batch header (first 8 bytes) before writing.
- The batch header carries a **CRC32C** (Castagnoli) over the bytes after the
  crc field — the tests re-check it, so your stored bytes must stay valid.
- Advance the log-end offset by the record count.

Tests: a real producer gets contiguous offsets (0,1,2,...), a second produce
continues the offsets, and the stored batch is magic=2 with a valid CRC32C.
""",
    4: """# Stage 4 — Fetch by offset

Implement **Fetch** (api 1) and **ListOffsets** (api 2) so a real consumer can
read your log.

- Return the raw record batches covering offsets `>= fetch_offset` in the
  `records` field; use the offset index to seek and skip batches below the
  requested offset.
- Report `high_watermark`, `last_stable_offset`, `log_start_offset`.
- ListOffsets answers earliest (ts −2) and latest (ts −1).

Tests: a manually-assigned consumer reads everything from offset 0, reads from a
mid-log offset, sees nothing past the end, and `get_watermark_offsets` returns
`(0, N)`.
""",
    5: """# Stage 5 — Metadata + multiple topics & partitions

Make the broker multi-topic and multi-partition.

- **Metadata** (api 3) lists each topic's partitions, with this broker as leader
  and sole replica/ISR. Unknown requested topics are auto-created.
- Key your logs by `(topic, partition)`; a Produce/Fetch names the partition.
- **CreateTopics** (api 19) lets `AdminClient.create_topics` set the partition
  count — store it and report it in Metadata.

Tests: two topics stay isolated; producing to partition 2 of a 3-partition
topic is readable only on partition 2; per-partition end offsets are
independent.
""",
    6: """# Stage 6 — Retention and log compaction

Topics carry a `cleanup.policy` (set through CreateTopics configs).

- `compact`: keep only the **latest record per key**, preserving each
  survivor's original offset, WITHOUT changing the log-end offset.
- `delete` (default): keep every version (optionally drop whole old segments by
  `retention.bytes`).

Compaction rewrites the log in place. Decode the records out of each v2 batch to
find the key of each record.

Tests: a compacted topic returns one record per key (the newest value); the log
end offset is unchanged; a delete-policy topic keeps all versions.
""",
    7: """# Stage 7 — Consumer-group offsets (OffsetCommit / OffsetFetch)

Let consumers checkpoint their progress.

- **FindCoordinator** (api 10): you are your own group coordinator.
- **OffsetCommit** (api 8): store an offset per `(group, topic, partition)`.
- **OffsetFetch** (api 9): read it back; return −1 when there is no commit.

Mind the request shapes: OffsetFetch v8+ uses a `groups` array and v9 adds
`member_id` + `member_epoch` before the topics.

Tests: commit then fetch in the same group (fresh consumer); unknown group has
no offset; two groups keep independent offsets.
""",
    8: """# Stage 8 — Replication & the high watermark

In a replicated cluster the **high watermark (HW)** is the offset up to which
records are committed on enough replicas to be visible to consumers. On your
single node there are no followers, so once a record is durably written
(flush + fsync) the HW advances to the log-end offset.

- Track a per-partition HW; advance it on durable append.
- Serve Fetch / ListOffsets(latest) from the HW and never return records at or
  beyond it.
- Restore the HW on startup so it (and all offsets) survive a restart from the
  same data dir.

Tests: HW equals the produced record count; a consumer reads everything up to
the HW; HW and data survive a restart.
""",
}


def write_stage(num, slug):
    sdir = CH / f"stage-{num:02d}-{slug}"
    (sdir / "tests").mkdir(parents=True, exist_ok=True)
    (sdir / "solution").mkdir(parents=True, exist_ok=True)
    (sdir / "alt_solutions" / "threaded").mkdir(parents=True, exist_ok=True)

    # stage.yaml
    hints_yaml = "\n".join(
        "  - " + _yaml_scalar(h) for h in HINTS[num])
    (sdir / "stage.yaml").write_text(
        f"title: {_yaml_scalar(TITLES[num])}\n"
        f"entry: broker.py\n"
        f"timeout_s: {TIMEOUT[num]}\n"
        f"hints:\n{hints_yaml}\n")

    (sdir / "README.md").write_text(README[num])

    # tests
    shutil.copyfile(HERE / "conftest_template.py", sdir / "tests" / "conftest.py")
    shutil.copyfile(HERE / "_broker_harness.py", sdir / "tests" / "_broker_harness.py")
    shutil.copyfile(HERE / f"test_stage{num}.py", sdir / "tests" / f"test_stage{num}.py")

    # cumulative solution + alt (same full impl passes 1..N for all N)
    shutil.copyfile(HERE / "broker_full.py", sdir / "solution" / "broker.py")
    shutil.copyfile(HERE / "broker_alt.py",
                    sdir / "alt_solutions" / "threaded" / "broker.py")


def _yaml_scalar(s: str) -> str:
    s = s.replace('"', '\\"')
    return f'"{s}"'


def main():
    for num, slug in STAGES:
        write_stage(num, slug)
        print(f"wrote stage-{num:02d}-{slug}")


if __name__ == "__main__":
    main()
