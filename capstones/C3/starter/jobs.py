"""C3 starter — the building blocks the learner must implement.

Each function returns a ``dict`` with ``ddl`` (list[str]) and ``inserts``
(list[str]) that the acceptance tests feed to the ``paimon_sql`` fixture, plus the
``query`` the oracle compares. Keeping it as plain SQL text keeps the harness
engine-agnostic and lets alts use the Flink Table API instead if desired.

The bodies below are INTENTIONALLY INCOMPLETE so the acceptance tests fail (G1).
Implement them (mirror the SQL stubs under ../sql and ../ddl). Do NOT copy
../reference — build your own.
"""
from __future__ import annotations


def golden_record(records: list[dict]) -> dict:
    """M2 — Paimon partial-update golden record (MDM).

    ``records`` are per-source partial rows: ``{match_key, customer_id, name,
    email, phone, loyalty_tier}`` with Nones for fields a source doesn't provide.
    Return {ddl, inserts, query} building ONE golden row per match_key.
    """
    # TODO(learner): use merge-engine='partial-update', bucket='1'; one INSERT per
    # record keeping non-null fields. The stub below uses the WRONG merge engine
    # (deduplicate) so later writes overwrite earlier non-null fields -> G1 fails.
    ddl = [
        "CREATE TABLE golden_customer ("
        "match_key STRING, customer_id INT, name STRING, email STRING, "
        "phone STRING, loyalty_tier STRING, PRIMARY KEY(match_key) NOT ENFORCED)"
        " WITH ('bucket'='1')"  # BUG: no partial-update merge engine
    ]
    inserts = []
    for r in records:
        inserts.append(
            "INSERT INTO golden_customer VALUES ("
            f"{_s(r.get('match_key'))},{_i(r.get('customer_id'))},"
            f"{_s(r.get('name'))},{_s(r.get('email'))},"
            f"{_s(r.get('phone'))},{_s(r.get('loyalty_tier'))})"
        )
    return {"ddl": ddl, "inserts": inserts,
            "query": "SELECT match_key,name,email,phone,loyalty_tier "
                     "FROM golden_customer"}


def fct_orders_fx(orders: list[dict], fx_rates: list[dict]) -> dict:
    """M3 — fct_orders enriched at the FX rate as of order time.

    Return {ddl, inserts, query} where usd_amount = amount * (rate in effect at
    order_ts). The stub multiplies by the LATEST rate (wrong) -> G1 fails.
    """
    # TODO(learner): pick the fx row with the greatest rate_date <= order date.
    raise NotImplementedError("implement fct_orders_fx (M3)")


def _s(v) -> str:
    return "CAST(NULL AS STRING)" if v is None else "'" + str(v).replace("'", "''") + "'"


def _i(v) -> str:
    return "CAST(NULL AS INT)" if v is None else str(int(v))
