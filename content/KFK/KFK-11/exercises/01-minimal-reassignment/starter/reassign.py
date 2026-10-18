"""Starter — KFK-11. Implement the minimal-movement reassignment planner and the
apply helpers. The stubs below currently fail the tests (gate G1)."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path


def plan_reassignment(current, broker_set, rf, racks=None):
    """Return {partition: [broker, ...]} with exactly rf distinct replicas from
    broker_set, rack-distinct when racks are given and rf <= number of racks,
    keeping as many current replicas as possible (minimal data movement)."""
    # TODO
    return {}


def to_assignment_json(plan, topic):
    """Return {'version':1,'partitions':[{'topic','partition','replicas'}, ...]}."""
    # TODO
    return {}


def apply_reassignment(cluster, topic, plan, timeout=90.0):
    """Write the plan JSON, run kafka-reassign-partitions.sh --execute, then poll
    --verify until the reassignment is complete. Return True on completion."""
    # TODO
    return False
