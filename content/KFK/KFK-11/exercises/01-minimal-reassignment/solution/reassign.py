"""Reference solution — KFK-11 minimal-movement partition reassignment.

Pure planner (``plan_reassignment``) graded with Hypothesis properties + a
movement oracle, plus helpers to serialise the plan and apply it to a live
multi-broker cluster via ``kafka-reassign-partitions.sh``.
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path


def plan_reassignment(current, broker_set, rf, racks=None):
    """Compute a new replica assignment that:

      * has exactly ``rf`` DISTINCT replicas per partition, all in ``broker_set``;
      * when ``racks`` is given and ``rf <= number_of_racks``, places every
        replica of a partition on a DISTINCT rack;
      * moves as FEW replicas as possible: keeps every current replica that is
        still valid under the target broker set (and rack rule), replacing only
        what it must and balancing load across brokers.

    ``current``: {partition: [broker, ...]}.  Returns {partition: [broker, ...]}.
    """
    broker_set = list(broker_set)
    rack_of = dict(racks) if racks else {}
    n_racks = len({rack_of[b] for b in broker_set if b in rack_of}) if rack_of else 0
    enforce_rack = bool(rack_of) and rf <= n_racks and n_racks > 0

    # running load count so we spread new replicas across the least-used brokers
    load = {b: 0 for b in broker_set}

    plan = {}
    for p in sorted(current):
        chosen = []
        used_racks = set()

        def can_take(b):
            if b in chosen:
                return False
            if b not in broker_set:
                return False
            if enforce_rack and rack_of.get(b) in used_racks:
                return False
            return True

        # 1) keep still-valid current replicas (zero data movement), in order
        for b in current[p]:
            if len(chosen) >= rf:
                break
            if can_take(b):
                chosen.append(b)
                if enforce_rack:
                    used_racks.add(rack_of.get(b))

        # 2) fill the rest from the least-loaded eligible brokers
        while len(chosen) < rf:
            candidates = [b for b in broker_set if can_take(b)]
            if not candidates:
                # rack rule can't be met with distinct racks; relax it so we
                # still return a valid (distinct-broker) assignment.
                candidates = [b for b in broker_set if b not in chosen]
                if not candidates:
                    break
            candidates.sort(key=lambda b: (load[b], b))
            pick = candidates[0]
            chosen.append(pick)
            if enforce_rack:
                used_racks.add(rack_of.get(pick))

        for b in chosen:
            load[b] = load.get(b, 0) + 1
        plan[p] = chosen
    return plan


def to_assignment_json(plan, topic):
    """Build the dict kafka-reassign-partitions.sh --execute expects."""
    return {
        "version": 1,
        "partitions": [
            {"topic": topic, "partition": p, "replicas": list(replicas)}
            for p, replicas in sorted(plan.items())
        ],
    }


def apply_reassignment(cluster, topic, plan, timeout=90.0):
    """Write the plan JSON and run --execute, then poll --verify until complete.

    ``cluster`` is a de_testkit KafkaCluster (``run_tool`` + ``bootstrap_servers``).
    Returns True when the reassignment verifies complete.
    """
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
