"""SPK-11 / 03-streaming-scd2-merge — reference solution (two MERGE passes)."""

SCHEMA = "cust_key INT, attr STRING, change_ts TIMESTAMP"


def _apply_scd2(table):
    def fn(batch_df, batch_id):
        spark = batch_df.sparkSession
        # One change per key per batch (idempotent, well-defined ordering of effects).
        batch_df.dropDuplicates(["cust_key"]).createOrReplaceTempView("scd2_changes")

        # Pass 1: close the current open version when the attribute actually changed.
        spark.sql(f"""
            MERGE INTO {table} t
            USING scd2_changes c
            ON t.cust_key = c.cust_key AND t.is_current = true
            WHEN MATCHED AND t.attr <> c.attr
              THEN UPDATE SET t.valid_to = c.change_ts, t.is_current = false
        """)

        # Pass 2: insert a new open version for new keys and for just-closed keys.
        spark.sql(f"""
            MERGE INTO {table} t
            USING scd2_changes c
            ON t.cust_key = c.cust_key AND t.is_current = true
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
        .foreachBatch(_apply_scd2(table))
        .start()
    )
    query.processAllAvailable()
    query.stop()
