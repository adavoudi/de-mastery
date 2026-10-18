"""Mutant: no-key — publishes records WITHOUT a key, so they spread across
partitions and a single key's events are no longer ordered within one partition.
The config is fine; the publish breaks per-key ordering."""


def tune_for_throughput():
    return {
        "enable.idempotence": True,
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
    for _key, value in records:
        p.produce(topic, value=value)   # BUG: no key -> cross-partition spread
    p.flush(20)
