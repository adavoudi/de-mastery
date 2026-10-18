"""Alt 1 (genuinely different): zstd compression, in-flight pinned to 1 (the
strictest ordering choice), larger linger; publish with a poll()-driven loop."""


def tune_for_throughput():
    conf = {}
    conf["enable.idempotence"] = True
    conf["acks"] = "all"
    conf["max.in.flight.requests.per.connection"] = 1
    conf["linger.ms"] = 25
    conf["batch.size"] = 262144        # 256 KiB
    conf["compression.type"] = "zstd"
    return conf


def publish_ordered(bootstrap_servers, topic, records):
    from confluent_kafka import Producer

    conf = {**tune_for_throughput(), "bootstrap.servers": bootstrap_servers}
    p = Producer(conf)
    for key, value in records:
        while True:
            try:
                p.produce(topic, key=key, value=value)
                break
            except BufferError:
                p.poll(0.1)
        p.poll(0)
    p.flush(20)
