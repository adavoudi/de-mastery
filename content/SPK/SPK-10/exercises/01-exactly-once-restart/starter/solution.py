"""SPK-10 / 01-exactly-once-restart — starter. See README.md.

Stream the JSON file source in `source_dir` into the Iceberg `table` with
checkpointing so restarts are exactly-once.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    # TODO: readStream the JSON source with SCHEMA, writeStream.toTable(table) in
    # append mode, with checkpointLocation=checkpoint and trigger(availableNow=True),
    # then processAllAvailable() and stop().
    raise NotImplementedError
