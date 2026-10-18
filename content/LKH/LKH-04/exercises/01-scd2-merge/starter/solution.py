"""LKH-04 / 01-scd2-merge — starter. See README.md."""


def apply_scd2(spark, dim_ident, src_values_sql, batch_date):
    # TODO: step 1 close changed current rows (valid_to=batch_date, is_current=false)
    # via MERGE; step 2 insert a new open version for changed + new keys. Step 1 first.
    raise NotImplementedError
