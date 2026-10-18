"""Mutant: create_table swaps the modes (CoW when asked MoR, and vice versa)."""


def create_table(spark, ident, mode):
    assert mode in ("copy-on-write", "merge-on-read")
    # WRONG: invert the requested mode.
    actual = "merge-on-read" if mode == "copy-on-write" else "copy-on-write"
    spark.sql(
        f"CREATE TABLE {ident} (id INT, cc STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2', "
        f"'write.delete.mode' = '{actual}', "
        f"'write.update.mode' = '{actual}', "
        f"'write.merge.mode'  = '{actual}')"
    )


def files_by_content(spark, ident):
    rows = spark.sql(
        f"SELECT content, count(*) AS c FROM {ident}.files GROUP BY content"
    ).collect()
    return {int(r["content"]): int(r["c"]) for r in rows}


def data_file_paths(spark, ident):
    rows = spark.sql(f"SELECT file_path FROM {ident}.files WHERE content = 0").collect()
    return {r["file_path"] for r in rows}
