"""Mutant: a hand-rolled, non-idempotent sink that re-reads and re-appends all files each call."""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    # WRONG: no streaming offsets at all; every call re-reads the whole directory and
    # appends it again, so re-running duplicates everything already loaded.
    df = spark.read.schema(SCHEMA).format("json").load(source_dir)
    df.writeTo(table).append()
