"""SPK-10 / 01-exactly-once-restart — alternate: foreachBatch idempotent append.

Each committed micro-batch appends via the DataFrameWriterV2 (writeTo(...).append()).
The checkpoint tracks which batches committed, so a clean restart only runs
uncommitted batches — no duplicates across stop/restart. Genuinely different from the
declarative table sink.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)

    def append_batch(batch_df, batch_id):
        batch_df.writeTo(table).append()

    query = (
        stream.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .foreachBatch(append_batch)
        .start()
    )
    query.processAllAvailable()
    query.stop()
