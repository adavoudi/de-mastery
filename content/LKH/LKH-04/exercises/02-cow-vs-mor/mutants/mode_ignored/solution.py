"""Mutant: create_table ignores `mode` and always makes a copy-on-write table.

So a table that should be merge-on-read never produces a delete file (no content=1).
"""


def create_table(spark, ident, mode):
    # WRONG: mode ignored; default (copy-on-write) always used.
    spark.sql(
        f"CREATE TABLE {ident} (id INT, cc STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2', "
        f"'write.delete.mode' = 'copy-on-write', "
        f"'write.update.mode' = 'copy-on-write', "
        f"'write.merge.mode'  = 'copy-on-write')"
    )


def files_by_content(spark, ident):
    rows = spark.sql(
        f"SELECT content, count(*) AS c FROM {ident}.files GROUP BY content"
    ).collect()
    return {int(r["content"]): int(r["c"]) for r in rows}


def data_file_paths(spark, ident):
    rows = spark.sql(f"SELECT file_path FROM {ident}.files WHERE content = 0").collect()
    return {r["file_path"] for r in rows}
