"""Mutant: profile REPLACES the config instead of merging, dropping BASE keys."""

from pyspark.sql import functions as F

_BASE = {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}
_PROFILES = {
    "dev": {"spark.sql.shuffle.partitions": "8"},
    "prod": {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "400"},
}


def build_config(env, overrides=None):
    # WRONG: if a profile exists, use ONLY it (BASE keys like session.timeZone are lost).
    conf = dict(_PROFILES[env]) if env in _PROFILES else dict(_BASE)
    conf.update(overrides or {})
    return conf


def revenue_report(orders_df):
    return orders_df.groupBy("company_id").agg(F.sum("line_amount").alias("revenue"))
