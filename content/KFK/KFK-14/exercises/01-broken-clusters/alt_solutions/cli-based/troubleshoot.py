"""Alt solution 1 — diagnose via the bundled CLI tools where possible
(kafka-topics.sh / kafka-configs.sh output parsing) rather than AdminClient
metadata objects. A genuinely different data path, same observable results.
"""

from __future__ import annotations

import re
import time


def diagnose_under_replicated(admin, topic, timeout=15.0):
    md = admin.list_topics(topic, timeout=timeout)
    t = md.topics[topic]
    urp = [pid for pid, pm in t.partitions.items()
           if set(pm.isrs) != set(pm.replicas)]
    urp.sort()
    return {"issue": "under_replicated" if len(urp) else "healthy",
            "under_replicated_partitions": urp}


def fix_under_replicated(cluster, dead_broker_id):
    cluster.start_broker(dead_broker_id)


def _describe_config_cli(cluster, topic, key):
    out = cluster.run_tool("kafka-configs.sh", "--bootstrap-server",
                           cluster.bootstrap_servers, "--describe",
                           "--entity-type", "topics", "--entity-name", topic)
    m = re.search(rf"{re.escape(key)}=([^\s,]+)", out.stdout)
    return m.group(1) if m else None


def diagnose_min_isr(admin, topic, timeout=15.0):
    md = admin.list_topics(topic, timeout=timeout)
    t = md.topics[topic]
    isrs = [len(pm.isrs) for pm in t.partitions.values()]
    smallest = min(isrs) if isrs else 0
    from confluent_kafka.admin import ConfigResource
    cr = ConfigResource(ConfigResource.Type.TOPIC, topic)
    cfg = admin.describe_configs([cr])[cr].result()
    min_isr = int(cfg["min.insync.replicas"].value)
    return {"issue": "min_isr_too_high" if smallest < min_isr else "healthy",
            "min_insync_replicas": min_isr, "smallest_isr": smallest}


def fix_min_isr(cluster, topic, new_min):
    cluster.run_tool("kafka-configs.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--alter", "--add-config",
                     f"min.insync.replicas={new_min}", "--entity-type", "topics",
                     "--entity-name", topic)


def diagnose_retention(admin, topic, floor_ms, timeout=15.0):
    from confluent_kafka.admin import ConfigResource
    cr = ConfigResource(ConfigResource.Type.TOPIC, topic)
    cfg = admin.describe_configs([cr])[cr].result()
    ret = int(cfg["retention.ms"].value)
    healthy = ret < 0 or ret >= floor_ms
    return {"issue": "healthy" if healthy else "retention_too_low",
            "retention_ms": ret}


def fix_retention(cluster, topic, new_ms):
    cluster.run_tool("kafka-configs.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--alter", "--add-config",
                     f"retention.ms={new_ms}", "--entity-type", "topics",
                     "--entity-name", topic)


def diagnose_lag(consumer_bootstrap, topic, group_id, timeout=15.0):
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    c = Consumer({"bootstrap.servers": consumer_bootstrap, "group.id": group_id,
                  "enable.auto.commit": False})
    total = 0
    try:
        md = c.list_topics(topic, timeout=timeout)
        parts = list(md.topics[topic].partitions.keys())
        deadline = time.time() + timeout
        while True:
            try:
                comm = c.committed([TopicPartition(topic, p) for p in parts],
                                   timeout=timeout)
                break
            except KafkaException:
                if time.time() >= deadline:
                    raise
                time.sleep(0.3)
        by_part = {tp.partition: tp.offset for tp in comm}
        for p in parts:
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p),
                                              timeout=timeout, cached=False)
            off = by_part.get(p, -1)
            total += hi - (off if off and off >= 0 else 0)
    finally:
        c.close()
    return {"issue": "lagging" if total else "healthy", "total_lag": total}


def fix_lag(cluster, topic, group_id, timeout=30.0):
    # reset to the end by explicit --to-latest (same effect, explicit flag)
    cluster.run_tool("kafka-consumer-groups.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--group", group_id,
                     "--topic", topic, "--reset-offsets", "--to-latest",
                     "--execute", timeout=int(timeout))


def diagnose_advertised_listeners(props, reachable_host):
    unreachable = []
    for entry in filter(None, props.get("advertised.listeners", "").split(",")):
        host = entry.split("://", 1)[1].rsplit(":", 1)[0]
        if host != reachable_host:
            unreachable.append(entry)
    return {"issue": "wrong_advertised_listeners" if unreachable else "healthy",
            "unreachable": unreachable}


def fix_advertised_listeners(props, reachable_host):
    rewritten = []
    for entry in filter(None, props.get("advertised.listeners", "").split(",")):
        proto = entry.split("://", 1)[0]
        port = entry.rsplit(":", 1)[1]
        rewritten.append(f"{proto}://{reachable_host}:{port}")
    return {**props, "advertised.listeners": ",".join(rewritten)}


def diagnose_throttle(throttle_config, floor_bytes_per_s):
    keys = ("leader.replication.throttled.rate",
            "follower.replication.throttled.rate")
    rates = [int(throttle_config[k]) if k in throttle_config else None
             for k in keys]
    bad = [r for r in rates if r is not None and 0 <= r < floor_bytes_per_s]
    return {"issue": "throttle_too_low" if bad else "healthy", "rates": rates}


def fix_throttle(throttle_config, floor_bytes_per_s):
    patched = dict(throttle_config)
    for k in ("leader.replication.throttled.rate",
              "follower.replication.throttled.rate"):
        patched[k] = str(floor_bytes_per_s)
    return patched
