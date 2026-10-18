"""Mutant: ignores the provided checkpoint and makes a fresh one each run."""

import tempfile

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    fresh = tempfile.mkdtemp()  # WRONG: new checkpoint each call -> re-ingest everything
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    query = (
        stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", fresh)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
