"""Mutant: writes df as-is without setting the partition count, so the file count
is whatever the input happened to have (one file from a single-file read), not
num_files."""


def write_balanced(df, out_dir, num_files):
    out = str(out_dir)
    df.write.mode("overwrite").parquet(out)
    return out
