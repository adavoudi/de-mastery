"""SPK-07 / 01 — alt: overshoot the partition count then coalesce down to exactly
num_files. Different mechanism (coalesce merge) but the same resulting file count."""


def write_balanced(df, out_dir, num_files):
    out = str(out_dir)
    (
        df.repartition(num_files * 4)
        .coalesce(num_files)
        .write.mode("overwrite")
        .parquet(out)
    )
    return out
