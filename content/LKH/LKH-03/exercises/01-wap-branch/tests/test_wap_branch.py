"""Tests for LKH-03 / 01-wap-branch.

A good batch must publish (main grows); a bad batch must be blocked (main unchanged
and the bad rows never appear on main). Uses the shared Iceberg Spark session, so
each test uses a UNIQUE table name to stay independent. SKIPS without the JARs.
"""

from __future__ import annotations

import uuid

import pytest


def _audit_nonneg(df):
    """Quality gate: no negative amounts allowed."""
    return df.filter("amt < 0").count() == 0


@pytest.fixture
def orders(spark_iceberg):
    s = spark_iceberg
    s.sql("CREATE NAMESPACE IF NOT EXISTS local.wap")
    name = f"local.wap.orders_{uuid.uuid4().hex[:8]}"
    s.sql(f"CREATE TABLE {name} (id INT, amt INT) USING iceberg")
    s.sql(f"INSERT INTO {name} VALUES (1, 10), (2, 20)")   # 2-row baseline on main
    yield s, name
    s.sql(f"DROP TABLE IF EXISTS {name} PURGE")


def test_good_batch_publishes(submission, orders):
    s, name = orders
    res = submission.write_audit_publish(s, name, "VALUES (3, 30), (4, 40)", _audit_nonneg)
    assert res["published"] is True
    assert res["branch_count"] == 4           # 2 baseline + 2 new, seen on the branch
    assert res["main_count"] == 4             # published to main
    assert s.sql(f"SELECT count(*) c FROM {name}").collect()[0]["c"] == 4


def test_bad_batch_blocked(submission, orders):
    s, name = orders
    res = submission.write_audit_publish(s, name, "VALUES (3, -5)", _audit_nonneg)
    assert res["published"] is False
    # main must be untouched: still exactly the 2 baseline rows.
    assert res["main_count"] == 2
    assert s.sql(f"SELECT count(*) c FROM {name}").collect()[0]["c"] == 2
    # the bad row must NOT be on main.
    assert s.sql(f"SELECT count(*) c FROM {name} WHERE amt < 0").collect()[0]["c"] == 0


def test_main_isolated_during_audit(submission, orders):
    """Even a good batch must not expose its rows to main before publish; after the
    flow, main holds exactly the published set and nothing partial."""
    s, name = orders
    before = s.sql(f"SELECT count(*) c FROM {name}").collect()[0]["c"]
    assert before == 2
    submission.write_audit_publish(s, name, "VALUES (5, 50)", _audit_nonneg)
    assert s.sql(f"SELECT count(*) c FROM {name}").collect()[0]["c"] == 3


def test_branch_cleaned_up(submission, orders):
    s, name = orders
    submission.write_audit_publish(s, name, "VALUES (9, 90)", _audit_nonneg)
    refs = [r["name"] for r in s.sql(f"SELECT name FROM {name}.refs").collect()]
    assert "audit" not in refs, "staging branch must be dropped"


def test_bad_then_good_sequence(submission, orders):
    """A blocked bad batch must not poison a later good batch."""
    s, name = orders
    submission.write_audit_publish(s, name, "VALUES (3, -1)", _audit_nonneg)
    assert s.sql(f"SELECT count(*) c FROM {name}").collect()[0]["c"] == 2  # still blocked
    res = submission.write_audit_publish(s, name, "VALUES (3, 30)", _audit_nonneg)
    assert res["published"] is True
    assert s.sql(f"SELECT count(*) c FROM {name}").collect()[0]["c"] == 3
    assert s.sql(f"SELECT count(*) c FROM {name} WHERE amt < 0").collect()[0]["c"] == 0
