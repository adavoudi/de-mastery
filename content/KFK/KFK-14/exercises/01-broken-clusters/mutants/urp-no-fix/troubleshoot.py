"""Reference solution — KFK-14 six broken-cluster scenarios.

Each incident is a diagnose_X (reads state, returns a small structured dict) and
a fix_X (applies the smallest change). Four scenarios run against a native
cluster; two (advertised listeners, throttle) are pure config diagnoses.
"""

from __future__ import annotations

import time


# ===========================================================================
# 1. Under-replicated partitions (ISR shorter than the replica list)
# ===========================================================================
def diagnose_under_replicated(admin, topic, timeout=15.0):
    md = admin.list_topics(topic, timeout=timeout)
    t = md.topics[topic]
    urp = sorted(pid for pid, pm in t.partitions.items()
                 if len(pm.isrs) < len(pm.replicas))
    return {"issue": "under_replicated" if urp else "healthy",
            "under_replicated_partitions": urp}


def fix_under_replicated(cluster, dead_broker_id):
    """Bring the offline broker back so its replicas re-sync."""
    # BUG: does nothing — the dead broker is never restarted, so URP persists.
    pass


# ===========================================================================
# 2. min.insync.replicas higher than the available in-sync replicas
# ===========================================================================
def _topic_config(admin, topic, timeout=15.0):
    from confluent_kafka.admin import ConfigResource
    cr = ConfigResource(ConfigResource.Type.TOPIC, topic)
    res = admin.describe_configs([cr])[cr].result()
    return {k: v.value for k, v in res.items()}


def diagnose_min_isr(admin, topic, timeout=15.0):
    md = admin.list_topics(topic, timeout=timeout)
    t = md.topics[topic]
    min_isr = int(_topic_config(admin, topic).get("min.insync.replicas", "1"))
    smallest_isr = min((len(pm.isrs) for pm in t.partitions.values()), default=0)
    bad = smallest_isr < min_isr
    return {"issue": "min_isr_too_high" if bad else "healthy",
            "min_insync_replicas": min_isr,
            "smallest_isr": smallest_isr}


def fix_min_isr(cluster, topic, new_min):
    cluster.run_tool("kafka-configs.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--alter", "--add-config",
                     f"min.insync.replicas={new_min}", "--entity-type", "topics",
                     "--entity-name", topic)


# ===========================================================================
# 3. Retention set below a sane floor
# ===========================================================================
def diagnose_retention(admin, topic, floor_ms, timeout=15.0):
    ret = int(_topic_config(admin, topic).get("retention.ms", "-1"))
    bad = 0 <= ret < floor_ms          # -1 == infinite retention is fine
    return {"issue": "retention_too_low" if bad else "healthy",
            "retention_ms": ret}


def fix_retention(cluster, topic, new_ms):
    cluster.run_tool("kafka-configs.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--alter", "--add-config",
                     f"retention.ms={new_ms}", "--entity-type", "topics",
                     "--entity-name", topic)


# ===========================================================================
# 4. Stuck / lagging consumer group (positioned behind the end)
# ===========================================================================
def diagnose_lag(consumer_bootstrap, topic, group_id, timeout=15.0):
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    c = Consumer({"bootstrap.servers": consumer_bootstrap, "group.id": group_id,
                  "enable.auto.commit": False})
    try:
        md = c.list_topics(topic, timeout=timeout)
        parts = list(md.topics[topic].partitions.keys())
        tps = [TopicPartition(topic, p) for p in parts]
        deadline = time.time() + timeout
        while True:
            try:
                committed = {tp.partition: tp.offset
                             for tp in c.committed(tps, timeout=timeout)}
                break
            except KafkaException:
                if time.time() >= deadline:
                    raise
                time.sleep(0.3)
        total = 0
        for p in parts:
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p),
                                              timeout=timeout, cached=False)
            off = committed.get(p, -1)
            off = off if off is not None and off >= 0 else 0
            total += max(0, hi - off)
    finally:
        c.close()
    return {"issue": "lagging" if total > 0 else "healthy", "total_lag": total}


def fix_lag(cluster, topic, group_id, timeout=30.0):
    """Reset the group to the latest offset so it stops being behind."""
    cluster.run_tool("kafka-consumer-groups.sh", "--bootstrap-server",
                     cluster.bootstrap_servers, "--group", group_id,
                     "--topic", topic, "--reset-offsets", "--to-latest",
                     "--execute", timeout=int(timeout))


# ===========================================================================
# 5. Wrong advertised.listeners (pure config diagnosis)
# ===========================================================================
def _split_listener(entry):
    # NAME://host:port  (or proto://host:port)
    proto, _, hostport = entry.partition("://")
    host, _, port = hostport.rpartition(":")
    return proto, host, port


def diagnose_advertised_listeners(props, reachable_host):
    adv = props.get("advertised.listeners", "")
    bad = []
    for entry in [e for e in adv.split(",") if e]:
        _proto, host, _port = _split_listener(entry)
        if host in ("0.0.0.0", "") or host != reachable_host:
            bad.append(entry)
    return {"issue": "wrong_advertised_listeners" if bad else "healthy",
            "unreachable": bad}


def fix_advertised_listeners(props, reachable_host):
    adv = props.get("advertised.listeners", "")
    fixed = []
    for entry in [e for e in adv.split(",") if e]:
        proto, _host, port = _split_listener(entry)
        fixed.append(f"{proto}://{reachable_host}:{port}")
    out = dict(props)
    out["advertised.listeners"] = ",".join(fixed)
    return out


# ===========================================================================
# 6. Throttled reassignment that never finishes (pure config diagnosis)
# ===========================================================================
def diagnose_throttle(throttle_config, floor_bytes_per_s):
    """``throttle_config`` is a dict like
    {"leader.replication.throttled.rate": "1024",
     "follower.replication.throttled.rate": "1024"} (values as strings; may be
    missing or "-1")."""
    rates = []
    for key in ("leader.replication.throttled.rate",
                "follower.replication.throttled.rate"):
        v = throttle_config.get(key)
        rates.append(int(v) if v is not None else None)
    too_low = any(r is not None and 0 <= r < floor_bytes_per_s for r in rates)
    return {"issue": "throttle_too_low" if too_low else "healthy",
            "rates": rates}


def fix_throttle(throttle_config, floor_bytes_per_s):
    out = dict(throttle_config)
    for key in ("leader.replication.throttled.rate",
                "follower.replication.throttled.rate"):
        out[key] = str(floor_bytes_per_s)
    return out
