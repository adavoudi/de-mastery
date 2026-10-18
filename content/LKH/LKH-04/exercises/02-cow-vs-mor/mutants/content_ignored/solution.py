"""Mutant: files_by_content ignores the content column, labelling every file as data.

So it never reports content=1, hiding the MoR delete file.
"""


def create_table(spark, ident, mode):
    assert mode in ("copy-on-write", "merge-on-read")
    spark.sql(
        f"CREATE TABLE {ident} (id INT, cc STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2', "
        f"'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', "
        f"'write.merge.mode'  = '{mode}')"
    )


def files_by_content(spark, ident):
    # WRONG: counts ALL files under content=0, ignoring the real content column.
    total = spark.sql(f"SELECT count(*) AS c FROM {ident}.files").collect()[0]["c"]
    return {0: int(total)}


def data_file_paths(spark, ident):
    rows = spark.sql(f"SELECT file_path FROM {ident}.files WHERE content = 0").collect()
    return {r["file_path"] for r in rows}
