"""Mutant: marks old row not-current but leaves its valid_to NULL (open end date)."""

SCHEMA = "cust_key INT, attr STRING, change_ts TIMESTAMP"


def _apply(table):
    def fn(batch_df, batch_id):
        spark = batch_df.sparkSession
        batch_df.dropDuplicates(["cust_key"]).createOrReplaceTempView("scd2_changes")
        spark.sql(f"""
            MERGE INTO {table} t
            USING scd2_changes c
            ON t.cust_key = c.cust_key AND t.is_current = true
            WHEN MATCHED AND t.attr <> c.attr
              THEN UPDATE SET t.is_current = false
        """)  # WRONG: does not set valid_to -> old version keeps valid_to = NULL
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
    query = (stream.writeStream.option("checkpointLocation", checkpoint)
             .trigger(availableNow=True).foreachBatch(_apply(table)).start())
    query.processAllAvailable()
    query.stop()
