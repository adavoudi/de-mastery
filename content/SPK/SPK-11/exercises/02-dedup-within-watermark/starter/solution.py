"""SPK-11 / 02-dedup-within-watermark — starter. See README.md.

Keep one row per event_id using a watermark-bounded dedup, appending to the Iceberg
table. Reuse the checkpoint.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def dedup(spark, source_dir, table, checkpoint):
    # TODO: readStream JSON (SCHEMA); withWatermark("event_ts","30 minutes");
    # dropDuplicatesWithinWatermark(["event_id"]); append -> toTable(table) with
    # checkpointLocation=checkpoint, trigger(availableNow=True); processAllAvailable();
    # stop().
    raise NotImplementedError
