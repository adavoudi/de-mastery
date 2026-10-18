"""Tests for KFK-09 / topic & key design.

Two graded surfaces:
  * a RUBRIC on design() (the defensible multi-tenant layout), and
  * PROPERTY tests (Hypothesis) on message_key / partition_for:
      1. per-entity ordering  — all events of one order_id -> one partition
      2. cross-process determinism — same event -> same partition in a fresh
         interpreter (kills a salted-hash() implementation)
      3. no hot partition under skew — a big tenant's many orders spread across
         (nearly) all partitions
      4. tenant isolation — distinct orders are not forced onto one partition by
         tenant alone (follows from high-cardinality keying)

The oracle for the rubric is the rule table below, each row traceable to the
lesson's cited Kafka docs (ordering-within-partition, log compaction, default
partitioner).
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

NO_FIXTURE_HC = [HealthCheck.function_scoped_fixture]


# --------------------------------------------------------------------------
# rubric on design()
# --------------------------------------------------------------------------
ORDERS_REQUIRED = {"per_entity_ordering", "avoid_hot_partition",
                   "high_cardinality_key"}
ORDERS_ANY = {"append_only_history", "time_retention"}
CUSTOMERS_REQUIRED = {"latest_state_per_key", "log_compaction"}
CUSTOMERS_ANY = {"rebuildable_from_log", "tenant_isolation"}

VOCAB = {
    "per_entity_ordering", "tenant_isolation", "avoid_hot_partition",
    "high_cardinality_key", "latest_state_per_key", "rebuildable_from_log",
    "append_only_history", "time_retention", "log_compaction",
}


def test_design_structure(submission):
    d = submission.design()
    assert set(d) >= {"orders", "customers"}
    for sec in ("orders", "customers"):
        for k in ("topic_type", "retention", "key", "factors"):
            assert k in d[sec], f"{sec} missing {k}"
        assert isinstance(d[sec]["factors"], list)
        assert set(d[sec]["factors"]) <= VOCAB, f"{sec}: unknown factor keys"


def test_orders_design(submission):
    o = submission.design()["orders"]
    assert o["topic_type"] == "event"
    assert o["retention"] == "time"
    assert o["key"] == "order_id"
    f = set(o["factors"])
    assert ORDERS_REQUIRED <= f, f"orders missing {ORDERS_REQUIRED - f}"
    assert f & ORDERS_ANY, f"orders needs one of {ORDERS_ANY}"


def test_customers_design(submission):
    c = submission.design()["customers"]
    assert c["topic_type"] == "entity"
    assert c["retention"] == "compacted"
    assert c["key"] == "customer_id"
    f = set(c["factors"])
    assert CUSTOMERS_REQUIRED <= f, f"customers missing {CUSTOMERS_REQUIRED - f}"
    assert f & CUSTOMERS_ANY, f"customers needs one of {CUSTOMERS_ANY}"


# --------------------------------------------------------------------------
# property tests on the partitioning function
# --------------------------------------------------------------------------
def _event(company, order, etype="created"):
    return {"company_id": company, "order_id": order, "event_type": etype,
            "ts": 1}


@settings(max_examples=60, deadline=None, suppress_health_check=NO_FIXTURE_HC)
@given(
    company=st.text(min_size=1, max_size=5),
    order=st.text(min_size=1, max_size=12),
    etypes=st.lists(st.sampled_from(["created", "paid", "shipped", "updated"]),
                    min_size=2, max_size=6),
    nparts=st.integers(min_value=2, max_value=24),
)
def test_per_entity_ordering(submission, company, order, etypes, nparts):
    """Every event for the same order maps to the same partition."""
    parts = {submission.partition_for(_event(company, order, e), nparts)
             for e in etypes}
    assert len(parts) == 1, f"events of order {order!r} split across {parts}"


@settings(max_examples=40, deadline=None, suppress_health_check=NO_FIXTURE_HC)
@given(
    company=st.text(min_size=1, max_size=5),
    order=st.text(min_size=1, max_size=12),
    nparts=st.integers(min_value=2, max_value=24),
)
def test_partition_in_range(submission, company, order, nparts):
    p = submission.partition_for(_event(company, order), nparts)
    assert 0 <= p < nparts


def test_no_hot_partition_under_skew(submission):
    """A single huge tenant with 2000 distinct orders must spread across (nearly)
    all partitions, not pile onto one. Keying by company_id would use exactly 1."""
    nparts = 12
    big = "MEGACORP"
    counts = [0] * nparts
    for i in range(2000):
        p = submission.partition_for(_event(big, f"{big}-ORD-{i}"), nparts)
        counts[p] += 1
    used = sum(1 for c in counts if c > 0)
    assert used >= nparts - 1, (
        f"big tenant used only {used}/{nparts} partitions (hot partition): {counts}")
    # no single partition holds more than ~25% of a well-spread 2000 (expected ~8%)
    assert max(counts) <= 0.25 * 2000, f"hot partition: {counts}"


def test_tenant_isolation_not_collapsed(submission):
    """Distinct orders across many tenants are not forced onto one partition by
    the tenant attribute alone — the key must be entity-derived."""
    nparts = 16
    parts = set()
    for t in range(50):
        for o in range(50):
            parts.add(submission.partition_for(
                _event(f"C{t:03d}", f"C{t:03d}-O{o:03d}"), nparts))
    # with entity-derived keys we expect to touch essentially all partitions
    assert len(parts) >= nparts - 1, f"only {len(parts)}/{nparts} partitions used"


def _partitions_in_subprocess(mod_file, events, nparts, hashseed):
    import json
    script = textwrap.dedent(f"""
        import importlib.util, json
        spec = importlib.util.spec_from_file_location("sub", {str(mod_file)!r})
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        ev = {events!r}
        out = [m.partition_for({{"company_id": c, "order_id": o,
                                 "event_type": "created", "ts": 1}}, {nparts})
               for c, o in ev]
        print(json.dumps(out))
    """)
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = str(hashseed)
    res = subprocess.run([sys.executable, "-c", script], capture_output=True,
                         text=True, env=env, timeout=30)
    assert res.returncode == 0, res.stderr
    return json.loads(res.stdout.strip().splitlines()[-1])


def test_cross_process_determinism(submission):
    """partition_for must be stable across interpreters (kills salted hash()).

    Compare two FRESH subprocesses with DIFFERENT PYTHONHASHSEEDs so the result
    is independent of the parent's seed: a salted-hash() implementation yields
    different partitions under different seeds and fails here.
    """
    sub_dir = os.environ["DE_SUBMISSION_DIR"]
    entry = os.environ.get("DE_SUBMISSION_ENTRY", "solution.py")
    mod_file = Path(sub_dir) / entry
    nparts = 10
    events = [("C001", f"O-{i}") for i in range(30)]

    seed_a = _partitions_in_subprocess(mod_file, events, nparts, 1)
    seed_b = _partitions_in_subprocess(mod_file, events, nparts, 999983)
    in_proc = [submission.partition_for(_event(c, o), nparts) for c, o in events]

    assert seed_a == seed_b, "partition_for depends on PYTHONHASHSEED (not stable)"
    assert in_proc == seed_a, "partition_for differs between parent and child process"
