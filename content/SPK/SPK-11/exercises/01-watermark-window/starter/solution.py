"""SPK-11 / 01-watermark-window — starter. See README.md.

Build an event-time windowed count with a 10-minute watermark and append finalized
windows (company_id, w_start, n) to the Iceberg table. Reuse the checkpoint.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def windowed_counts(spark, source_dir, table, checkpoint):
    # TODO: readStream JSON (SCHEMA); withWatermark("event_ts","10 minutes");
    # groupBy(window("event_ts","5 minutes"), "company_id").count();
    # select company_id, window.start AS w_start, count AS n;
    # writeStream append -> toTable(table), checkpointLocation=checkpoint,
    # trigger(availableNow=True); processAllAvailable(); stop().
    raise NotImplementedError
