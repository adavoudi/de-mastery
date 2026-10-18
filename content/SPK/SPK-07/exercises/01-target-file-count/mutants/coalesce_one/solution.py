"""Mutant: hard-codes coalesce(1), ignoring num_files — always writes 1 file."""


def write_balanced(df, out_dir, num_files):
    out = str(out_dir)
    df.coalesce(1).write.mode("overwrite").parquet(out)
    return out
