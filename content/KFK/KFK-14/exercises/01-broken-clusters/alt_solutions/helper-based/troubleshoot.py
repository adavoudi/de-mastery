"""Alt solution 2 — a shared config-reader helper and comprehension-based
idioms; lag reset via --to-latest. Same behaviour, different structure.
"""

from __future__ import annotations

import time


def _cfg(admin, topic):
    from confluent_kafka.admin import ConfigResource
    cr = ConfigResource(ConfigResource.Type.TOPIC, topic)
    return {k: v.value for k, v in admin.describe_configs([cr])[cr].result().items()}


def diagnose_under_replicated(admin, topic, timeout=15.0):
    parts = admin.list_topics(topic, timeout=timeout).topics[topic].partitions
    urp = sorted(p for p, pm in parts.items() if len(pm.isrs) < len(pm.replicas))
    return {"issue": "under_replicated" if urp else "healthy",
            "under_replicated_partitions": urp}


def fix_under_replicated(cluster, dead_broker_id):
    cluster.start_broker(dead_broker_id)


def diagnose_min_isr(admin, topic, timeout=15.0):
    parts = admin.list_topics(topic, timeout=timeout).topics[topic].partitions
    smallest = min((len(pm.isrs) for pm in parts.values()), default=0)
    min_isr = int(_cfg(admin, topic).get("min.insync.replicas", "1"))
    return {"issue": "min_isr_too_high" if smallest < min_isr else "healthy",
            "min_insync_replicas": min_isr, "smallest_isr": smallest}


def _alter(cluster, topic, kv):
    cluster.run_tool("kafka-configs.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--alter", "--add-config", kv,
                     "--entity-type", "topics", "--entity-name", topic)


def fix_min_isr(cluster, topic, new_min):
    _alter(cluster, topic, f"min.insync.replicas={new_min}")


def diagnose_retention(admin, topic, floor_ms, timeout=15.0):
    ret = int(_cfg(admin, topic).get("retention.ms", "-1"))
    bad = 0 <= ret < floor_ms
    return {"issue": "retention_too_low" if bad else "healthy", "retention_ms": ret}


def fix_retention(cluster, topic, new_ms):
    _alter(cluster, topic, f"retention.ms={new_ms}")


def diagnose_lag(consumer_bootstrap, topic, group_id, timeout=15.0):
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    c = Consumer({"bootstrap.servers": consumer_bootstrap, "group.id": group_id,
                  "enable.auto.commit": False})
    try:
        parts = list(c.list_topics(topic, timeout=timeout).topics[topic].partitions)
        end = time.time() + timeout
        while True:
            try:
                committed = c.committed([TopicPartition(topic, p) for p in parts],
                                        timeout=timeout)
                break
            except KafkaException:
                if time.time() >= end:
                    raise
                time.sleep(0.3)
        offs = {tp.partition: (tp.offset if tp.offset and tp.offset >= 0 else 0)
                for tp in committed}
        total = sum(
            max(0, c.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout,
                                           cached=False)[1] - offs.get(p, 0))
            for p in parts)
    finally:
        c.close()
    return {"issue": "lagging" if total > 0 else "healthy", "total_lag": total}


def fix_lag(cluster, topic, group_id, timeout=30.0):
    cluster.run_tool("kafka-consumer-groups.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--group", group_id,
                     "--topic", topic, "--reset-offsets", "--to-latest",
                     "--execute", timeout=int(timeout))


def diagnose_advertised_listeners(props, reachable_host):
    entries = [e for e in props.get("advertised.listeners", "").split(",") if e]
    bad = [e for e in entries
           if e.split("://")[1].rsplit(":", 1)[0] not in (reachable_host,)]
    return {"issue": "wrong_advertised_listeners" if bad else "healthy",
            "unreachable": bad}


def fix_advertised_listeners(props, reachable_host):
    def rewrite(e):
        proto = e.split("://")[0]
        port = e.rsplit(":", 1)[1]
        return f"{proto}://{reachable_host}:{port}"
    entries = [e for e in props.get("advertised.listeners", "").split(",") if e]
    return {**props, "advertised.listeners": ",".join(rewrite(e) for e in entries)}


def diagnose_throttle(throttle_config, floor_bytes_per_s):
    def rate(k):
        return int(throttle_config[k]) if k in throttle_config else None
    rates = [rate("leader.replication.throttled.rate"),
             rate("follower.replication.throttled.rate")]
    too_low = any(r is not None and 0 <= r < floor_bytes_per_s for r in rates)
    return {"issue": "throttle_too_low" if too_low else "healthy", "rates": rates}


def fix_throttle(throttle_config, floor_bytes_per_s):
    return {**throttle_config,
            "leader.replication.throttled.rate": str(floor_bytes_per_s),
            "follower.replication.throttled.rate": str(floor_bytes_per_s)}
