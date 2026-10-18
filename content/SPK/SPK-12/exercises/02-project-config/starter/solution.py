"""SPK-12 / 02-project-config — starter. See README.md."""

from pyspark.sql import functions as F


def build_config(env, overrides=None):
    # TODO: BASE defaults < per-env profile (dev/prod) < overrides. No secrets in BASE/profiles.
    raise NotImplementedError


def revenue_report(orders_df):
    # TODO: pure transform: per company_id, revenue = sum(line_amount).
    raise NotImplementedError
