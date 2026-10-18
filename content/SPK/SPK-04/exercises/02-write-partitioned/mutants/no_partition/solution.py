"""Mutant: forgets partitionBy, so there are no company_id=... directories."""


def write_partitioned(df, out_path):
    df.write.mode("overwrite").parquet(out_path)  # WRONG: not partitioned
