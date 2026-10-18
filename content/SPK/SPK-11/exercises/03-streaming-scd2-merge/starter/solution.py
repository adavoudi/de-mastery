"""SPK-11 / 03-streaming-scd2-merge — starter. See README.md.

Fold the change stream into a SCD2 Iceberg dimension using foreachBatch + MERGE INTO.
Close the old version and open a new one on a real change; do nothing on a repeat.
"""

SCHEMA = "cust_key INT, attr STRING, change_ts TIMESTAMP"


def upsert_scd2(spark, source_dir, table, checkpoint):
    # TODO: readStream JSON (SCHEMA). In foreachBatch(fn):
    #   - dedup the batch on cust_key, register it as a temp view,
    #   - MERGE pass 1: close current open rows whose attr changed
    #     (set valid_to = change_ts, is_current = false),
    #   - MERGE pass 2: insert a new open row for keys not currently open
    #     (valid_from = change_ts, valid_to = NULL, is_current = true).
    # Write with checkpointLocation=checkpoint, trigger(availableNow=True);
    # processAllAvailable(); stop().
    raise NotImplementedError
