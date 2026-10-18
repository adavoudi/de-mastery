"""Mutant: forgets partitionBy — writes a flat Parquet dir with no
company_id=<value>/ subdirectories, so the required layout is absent."""


def write_partitioned(df, out_dir):
    out = str(out_dir)
    df.write.mode("overwrite").parquet(out)
    return out
