"""Mutant: idempotence-off — leaves enable.idempotence False, so retries can
duplicate/reorder. The config no longer satisfies the no-duplicate guarantee."""


def tune_for_throughput():
    return {
        "enable.idempotence": False,   # BUG
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 10,
        "batch.size": 131072,
        "compression.type": "lz4",
    }


def publish_ordered(bootstrap_servers, topic, records):
    from confluent_kafka import Producer

    conf = {**tune_for_throughput(), "bootstrap.servers": bootstrap_servers}
    p = Producer(conf)
    for key, value in records:
        p.produce(topic, key=key, value=value)
    p.flush(20)
