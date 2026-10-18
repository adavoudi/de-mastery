"""Alt solution 2 — compute the kept set up front with set operations, then fill
remaining slots, breaking load ties by a global usage Counter. A different code
path from the reference's inline keep/fill loop.
"""

from __future__ import annotations

import json
import tempfile
import time
from collections import Counter
from pathlib import Path


def plan_reassignment(current, broker_set, rf, racks=None):
    broker_set = list(broker_set)
    rack_of = dict(racks) if racks else {}
    n_racks = len({rack_of[b] for b in broker_set if b in rack_of}) if rack_of else 0
    enforce = bool(rack_of) and rf <= n_racks and n_racks > 0
    usage = Counter()

    plan = {}
    for p in sorted(current):
        # kept = current replicas still in broker_set, respecting rack-distinct
        kept = []
        racks_used = set()
        for b in current[p]:
            if len(kept) >= rf:
                break
            if b not in broker_set or b in kept:
                continue
            if enforce and rack_of.get(b) in racks_used:
                continue
            kept.append(b)
            if enforce:
                racks_used.add(rack_of.get(b))

        chosen = list(kept)
        remaining = [b for b in broker_set if b not in chosen]
        # order fills by (rack already used?, usage, id) so we add new racks first
        def key(b):
            new_rack = 0 if (enforce and rack_of.get(b) not in racks_used) else 1
            return (new_rack, usage[b], b)

        while len(chosen) < rf and remaining:
            remaining.sort(key=key)
            # if enforcing racks, skip ones whose rack is taken unless we must
            pick = None
            for b in remaining:
                if enforce and rack_of.get(b) in racks_used:
                    continue
                pick = b
                break
            if pick is None:
                pick = remaining[0]  # relax rack rule to stay valid
            chosen.append(pick)
            remaining.remove(pick)
            if enforce:
                racks_used.add(rack_of.get(pick))

        for b in chosen:
            usage[b] += 1
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
