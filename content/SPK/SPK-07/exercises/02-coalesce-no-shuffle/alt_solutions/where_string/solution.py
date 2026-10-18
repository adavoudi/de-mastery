"""SPK-07 / 02 — alt: SQL-string filter, same narrow coalesce."""


def reduce_usd(df, num_files):
    return df.where("currency = 'USD'").coalesce(num_files)
