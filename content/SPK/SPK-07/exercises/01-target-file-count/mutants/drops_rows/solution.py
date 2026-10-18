"""Mutant: repartitions to the right count but silently drops rows (limit), so
the written data no longer equals the input — the data oracle fails."""


def write_balanced(df, out_dir, num_files):
    out = str(out_dir)
    df.limit(5).repartition(num_files).write.mode("overwrite").parquet(out)
    return out
