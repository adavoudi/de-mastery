"""Mutant: prod profile forgets to enable AQE."""

from pyspark.sql import functions as F

_BASE = {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}
_PROFILES = {
    "dev": {"spark.sql.shuffle.partitions": "8"},
    "prod": {"spark.sql.shuffle.partitions": "400"},  # WRONG: no adaptive.enabled=true
}


def build_config(env, overrides=None):
    conf = dict(_BASE)
    conf.update(_PROFILES.get(env, {}))
    conf.update(overrides or {})
    return conf


def revenue_report(orders_df):
    return orders_df.groupBy("company_id").agg(F.sum("line_amount").alias("revenue"))
