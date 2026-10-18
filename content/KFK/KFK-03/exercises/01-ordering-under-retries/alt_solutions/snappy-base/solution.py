"""Alt 2 (genuinely different): snappy compression, assembled from a base dict
plus overrides; publish by encoding values to bytes explicitly."""

_BASE = {
    "acks": "all",
    "linger.ms": 5,
    "batch.size": 32768,               # 32 KiB, still > 16384
}


def tune_for_throughput():
    return {
        **_BASE,
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 3,
        "compression.type": "snappy",
    }


def publish_ordered(bootstrap_servers, topic, records):
    from confluent_kafka import Producer

    p = Producer({**tune_for_throughput(), "bootstrap.servers": bootstrap_servers})
    for key, value in records:
        p.produce(topic,
                  key=key.encode() if isinstance(key, str) else key,
                  value=value.encode() if isinstance(value, str) else value)
    p.flush(20)
