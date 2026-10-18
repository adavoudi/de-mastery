"""Mutant: uses append instead of overwrite, so re-running doubles the data."""


def write_partitioned(df, out_path):
    (df.write
       .mode("append")  # WRONG: a refresh job must overwrite, not append
       .partitionBy("company_id")
       .parquet(out_path))
