"""Alt solution 1 — rack-bucketed planner. Group brokers by rack; for each
partition prefer to keep current replicas, then draw fills one-per-rack from the
least-loaded bucket. Serialisation/apply share the reference shape (there is one
correct JSON contract and one correct CLI invocation).
"""

from __future__ import annotations

import json
import tempfile
import time
from collections import defaultdict
from pathlib import Path


def plan_reassignment(current, broker_set, rf, racks=None):
    broker_set = list(broker_set)
    rack_of = dict(racks) if racks else {}
    buckets = defaultdict(list)  # rack -> [brokers]
    for b in broker_set:
        buckets[rack_of.get(b, b)].append(b)
    n_racks = len(buckets) if rack_of else 0
    enforce = bool(rack_of) and rf <= n_racks and n_racks > 0

    load = defaultdict(int)
    plan = {}
    for p in sorted(current):
        chosen = []
        used_racks = set()

        def rack(b):
            return rack_of.get(b, b)

        def ok(b):
            if b in chosen or b not in broker_set:
                return False
            if enforce and rack(b) in used_racks:
                return False
            return True

        for b in current[p]:            # keep reusable current replicas
            if len(chosen) >= rf:
                break
            if ok(b):
                chosen.append(b)
                used_racks.add(rack(b))
        while len(chosen) < rf:          # fill from least-loaded eligible broker
            cands = [b for b in broker_set if ok(b)]
            if not cands:
                cands = [b for b in broker_set if b not in chosen]
                if not cands:
                    break
            cands.sort(key=lambda b: (load[b], b))
            chosen.append(cands[0])
            used_racks.add(rack(cands[0]))
        for b in chosen:
            load[b] += 1
        plan[p] = chosen
    return plan


def to_assignment_json(plan, topic):
    return {
        "version": 1,
        "partitions": [
            {"topic": topic, "partition": p, "replicas": list(r)}
            for p, r in sorted(plan.items())
        ],
    }


def apply_reassignment(cluster, topic, plan, timeout=90.0):
    doc = to_assignment_json(plan, topic)
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(doc, f)
    f.close()
    bs = cluster.bootstrap_servers
    try:
        cluster.run_tool("kafka-reassign-partitions.sh", "--bootstrap-server", bs,
                         "--reassignment-json-file", f.name, "--execute")
        deadline = time.time() + timeout
        while time.time() < deadline:
            v = cluster.run_tool("kafka-reassign-partitions.sh",
                                 "--bootstrap-server", bs,
                                 "--reassignment-json-file", f.name,
                                 "--verify", check=False)
            if "is complete" in v.stdout or "completed successfully" in v.stdout:
                return True
            time.sleep(0.5)
        return False
    finally:
        Path(f.name).unlink(missing_ok=True)
