"""Tests for KFK-06 / evolve the order schema.

No-Docker graded path = the `check_avro_compatibility` fixture (pure fastavro),
plus a real produce/consume round-trip on a native `kafka_broker` where the SAME
new-schema bytes are read back with BOTH the new and the old reader schema. The
G6 oracle is an independent Python reconstruction of what each reader must see.

Each test uses a UNIQUE topic + fresh group so shuffled/parallel runs don't clash.
"""

import uuid

import pytest


def _topic():
    return f"kfk06-{uuid.uuid4().hex[:8]}"


def _orders():
    # (id, amount, currency); ids deliberately out of order to test sorting.
    return [(3, 30.0, "EUR"), (1, 10.0, "USD"), (2, 20.5, "GBP")]


# ---- no-Docker graded path: compatibility of the evolution ------------------

def test_base_schema_shape(submission):
    base = submission.base_schema()
    names = [f["name"] for f in base["fields"]]
    assert names == ["id", "amount"]


def test_evolution_adds_currency(submission):
    new = submission.evolved_schema()
    names = [f["name"] for f in new["fields"]]
    assert "currency" in names
    # must not drop or rename the original fields
    assert "id" in names and "amount" in names


def test_evolution_is_backward_compatible(submission, check_avro_compatibility):
    base = submission.base_schema()
    new = submission.evolved_schema()
    # BACKWARD: a reader on the NEW schema must read data written with the OLD one.
    assert check_avro_compatibility(new, base, "BACKWARD") is True


def test_a_required_field_would_not_be_backward(check_avro_compatibility):
    # sanity anchor on the oracle itself (independent of the learner)
    base = {"type": "record", "name": "Order", "namespace": "de.commerce",
            "fields": [{"name": "id", "type": "long"},
                       {"name": "amount", "type": "double"}]}
    bad = {**base, "fields": base["fields"] + [{"name": "r", "type": "string"}]}
    assert check_avro_compatibility(bad, base, "BACKWARD") is False


# ---- native broker round-trip -----------------------------------------------

def test_roundtrip_both_readers(kafka_broker, submission):
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=3, replication_factor=1)
    orders = _orders()

    out = submission.produce_and_read(kafka_broker.bootstrap_servers, topic, orders)

    assert set(out) == {"new_reader", "old_reader"}
    assert len(out["new_reader"]) == len(orders)
    assert len(out["old_reader"]) == len(orders)

    # G6 oracle: independent reconstruction of each reader's view.
    oracle_new = sorted(
        ({"id": i, "amount": a, "currency": c} for i, a, c in orders),
        key=lambda r: r["id"])
    oracle_old = sorted(
        ({"id": i, "amount": a} for i, a, _ in orders),
        key=lambda r: r["id"])

    assert out["new_reader"] == oracle_new
    # The old (not-yet-upgraded) consumer must NOT see currency at all.
    assert out["old_reader"] == oracle_old
    for rec in out["old_reader"]:
        assert "currency" not in rec


def test_old_reader_survives_evolution(kafka_broker, submission):
    """The core guarantee: new-schema bytes decode cleanly with the OLD schema."""
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    orders = [(10, 99.0, "JPY")]
    out = submission.produce_and_read(kafka_broker.bootstrap_servers, topic, orders)
    assert out["old_reader"] == [{"id": 10, "amount": 99.0}]
