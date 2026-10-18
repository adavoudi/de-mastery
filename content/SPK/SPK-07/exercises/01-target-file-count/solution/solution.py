"""SPK-07 / 01-target-file-count — reference solution."""


def write_balanced(df, out_dir, num_files):
    out = str(out_dir)
    (
        df.repartition(num_files)
        .write.mode("overwrite")
        .parquet(out)
    )
    return out
