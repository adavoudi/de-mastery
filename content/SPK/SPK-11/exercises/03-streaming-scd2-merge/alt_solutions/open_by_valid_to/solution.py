"""SPK-11 / 03-streaming-scd2-merge — alternate: 'open' detected by valid_to IS NULL.

Same two-pass MERGE, but the current/open row is identified by valid_to IS NULL
instead of is_current = true (both markers are kept consistent). Demonstrates that
either SCD2 marker works as the merge predicate.
"""

SCHEMA = "cust_key INT, attr STRING, change_ts TIMESTAMP"


def _apply(table):
    def fn(batch_df, batch_id):
        spark = batch_df.sparkSession
        batch_df.dropDuplicates(["cust_key"]).createOrReplaceTempView("scd2_changes")

        spark.sql(f"""
            MERGE INTO {table} t
            USING scd2_changes c
            ON t.cust_key = c.cust_key AND t.valid_to IS NULL
            WHEN MATCHED AND t.attr <> c.attr
              THEN UPDATE SET t.valid_to = c.change_ts, t.is_current = false
        """)
        spark.sql(f"""
            MERGE INTO {table} t
            USING scd2_changes c
            ON t.cust_key = c.cust_key AND t.valid_to IS NULL
            WHEN NOT MATCHED
              THEN INSERT (cust_key, attr, valid_from, valid_to, is_current)
                   VALUES (c.cust_key, c.attr, c.change_ts, NULL, true)
        """)
    return fn


def upsert_scd2(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    query = (
        stream.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .foreachBatch(_apply(table))
        .start()
    )
    query.processAllAvailable()
    query.stop()
