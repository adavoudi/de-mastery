"""C3 reference — a tiny PROVEN-CORRECT implementation.

This exists ONLY to prove the acceptance-test harness is valid (G2-style): the
tests pass when graded against ``reference`` and fail against ``starter``. It is
deliberately small (not the full capstone) — just enough to exercise each oracle.
Learners must not copy it.

Same contract as ``starter.jobs``: each function returns {ddl, inserts, query}.
"""
from __future__ import annotations


def golden_record(records: list[dict]) -> dict:
    """M2 — correct Paimon partial-update golden record."""
    ddl = [
        "CREATE TABLE golden_customer ("
        "match_key STRING, customer_id INT, name STRING, email STRING, "
        "phone STRING, loyalty_tier STRING, PRIMARY KEY(match_key) NOT ENFORCED)"
        " WITH ('merge-engine'='partial-update','bucket'='1')"
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
    """M3 — correct FX as-of enrichment, pre-resolved in Python then inserted.

    (The capstone does this with a Flink temporal join; the reference resolves the
    rate in Python so the harness proof stays tiny and deterministic.)
    """
    ddl = [
        "CREATE TABLE fct_orders ("
        "order_id INT, currency STRING, amount DOUBLE, fx_rate DOUBLE, "
        "usd_amount DOUBLE, PRIMARY KEY(order_id) NOT ENFORCED) "
        "WITH ('bucket'='1','merge-engine'='deduplicate')"
    ]
    inserts = []
    for o in orders:
        rate = _rate_as_of(fx_rates, o["currency"], o["order_date"])
        usd = o["amount"] * rate
        inserts.append(
            "INSERT INTO fct_orders VALUES ("
            f"{_i(o['order_id'])},{_s(o['currency'])},{o['amount']!r},"
            f"{rate!r},{usd!r})"
        )
    return {"ddl": ddl, "inserts": inserts,
            "query": "SELECT order_id,currency,amount,fx_rate,usd_amount "
                     "FROM fct_orders"}


def _rate_as_of(fx_rates: list[dict], currency: str, order_date: str) -> float:
    """Greatest rate_date <= order_date for the currency (the as-of semantics)."""
    cands = [r for r in fx_rates
             if r["currency"] == currency and r["rate_date"] <= order_date]
    if not cands:
        raise ValueError(f"no fx rate for {currency} as of {order_date}")
    return max(cands, key=lambda r: r["rate_date"])["usd_per_unit"]


def _s(v) -> str:
    return "CAST(NULL AS STRING)" if v is None else "'" + str(v).replace("'", "''") + "'"


def _i(v) -> str:
    return "CAST(NULL AS INT)" if v is None else str(int(v))
