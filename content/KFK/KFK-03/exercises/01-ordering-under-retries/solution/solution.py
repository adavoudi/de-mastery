"""Reference solution: a throughput-tuned, idempotent, order-preserving producer
config, and a keyed publisher."""


def tune_for_throughput():
    return {
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 10,
        "batch.size": 131072,          # 128 KiB, > 16384 default
        "compression.type": "lz4",
    }


def publish_ordered(bootstrap_servers, topic, records):
    from confluent_kafka import Producer

    conf = dict(tune_for_throughput())
    conf["bootstrap.servers"] = bootstrap_servers
    p = Producer(conf)
    for key, value in records:
        p.produce(topic, key=key, value=value)
    p.flush(20)
