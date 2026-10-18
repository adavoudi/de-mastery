"""SPK-10 / 02-output-modes — starter. See README.md.

Aggregate the file stream to per-company counts and write to the memory sink under
query_name with a VALID output mode for an aggregation. Return the stopped query.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_counts(spark, source_dir, query_name):
    # TODO: readStream JSON (SCHEMA) -> groupBy("company_id").count() aliased as n ->
    # writeStream.format("memory").queryName(query_name) with the correct outputMode,
    # trigger(availableNow=True); processAllAvailable(); stop(); return the query.
    raise NotImplementedError
