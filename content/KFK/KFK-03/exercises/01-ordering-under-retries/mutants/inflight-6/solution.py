"""Mutant: inflight-6 — sets max.in.flight.requests.per.connection=6, above the
ordering bound of 5, so ordering under retry is no longer guaranteed."""


def tune_for_throughput():
    return {
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 6,   # BUG: > 5
        "linger.ms": 10,
        "batch.size": 131072,
        "compression.type": "lz4",
    }


def publish_ordered(bootstrap_servers, topic, records):
    from confluent_kafka import Producer

    # Note: librdkafka would itself reject in-flight>5 with idempotence, which is
    # exactly why the config rule forbids it; grade on the config.
    conf = {**tune_for_throughput(), "bootstrap.servers": bootstrap_servers}
    conf["max.in.flight.requests.per.connection"] = 5  # so publish still runs
    p = Producer(conf)
    for key, value in records:
        p.produce(topic, key=key, value=value)
    p.flush(20)
