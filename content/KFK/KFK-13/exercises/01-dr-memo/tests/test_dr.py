"""Tests for KFK-13 / 01-dr-memo.

A machine-checkable rubric (like LKH-07), not prose grading. The learner edits
dr_design.yaml; this test reads it from DE_SUBMISSION_DIR and checks, per
scenario: the single defensible topology, that RPO/RTO fall in the band implied
by that topology, and that all required justification factors are present.

The rule table is the oracle; each rule traces to a doc cited in lesson.md
(MM2 async => RPO=lag>0; stretched synchronous => RPO~0 + >=3 failure domains;
active/active => both regions live, prefixed topics prevent loops).
"""

import os
from pathlib import Path

import yaml

VALID_TOPOLOGIES = {"active_passive", "active_active", "stretched"}

VOCAB = {
    "async_replication", "synchronous_replication", "warm_standby",
    "offset_translation", "three_failure_domains", "loop_prevention_prefix",
    "cross_region_latency", "local_low_latency", "mirrormaker2",
    "cluster_linking", "manual_cutover", "automated_cutover",
}

# (expected_topology, rpo_range_s, rto_range_s, required_factors)
RULES = {
    "scenario_1": ("active_passive", (1, 600), (60, 3600),
                   {"async_replication", "offset_translation", "warm_standby"}),
    "scenario_2": ("stretched", (0, 1), (0, 300),
                   {"synchronous_replication", "three_failure_domains"}),
    "scenario_3": ("active_active", (1, 600), (0, 120),
                   {"async_replication", "loop_prevention_prefix", "local_low_latency"}),
}


def _load():
    path = Path(os.environ["DE_SUBMISSION_DIR"]) / "dr_design.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def _factors(scn):
    raw = scn.get("factors") or []
    assert isinstance(raw, list), "factors must be a list"
    return set(raw)


def test_structure_valid():
    doc = _load()
    assert isinstance(doc, dict), "dr_design.yaml must be a mapping"
    for key in RULES:
        assert key in doc, f"missing {key}"
        for field in ("topology", "rpo_seconds", "rto_seconds", "factors"):
            assert field in doc[key], f"{key} missing '{field}'"


def test_topologies_in_vocabulary():
    doc = _load()
    for key in RULES:
        t = doc[key]["topology"]
        assert t in VALID_TOPOLOGIES, f"{key}: topology {t!r} not in {VALID_TOPOLOGIES}"


def test_factors_in_vocabulary():
    doc = _load()
    for key in RULES:
        extra = _factors(doc[key]) - VOCAB
        assert not extra, f"{key}: unknown factor keys {extra}"


def test_rpo_rto_are_numbers():
    doc = _load()
    for key in RULES:
        for field in ("rpo_seconds", "rto_seconds"):
            v = doc[key][field]
            assert isinstance(v, (int, float)) and not isinstance(v, bool), (
                f"{key}.{field} must be a number, got {v!r}")
            assert v >= 0, f"{key}.{field} must be >= 0"


def test_scenario_1_active_passive():
    _check(_load(), "scenario_1")


def test_scenario_2_stretched():
    _check(_load(), "scenario_2")


def test_scenario_3_active_active():
    _check(_load(), "scenario_3")


def _check(doc, key):
    expected_t, (rpo_lo, rpo_hi), (rto_lo, rto_hi), required = RULES[key]
    scn = doc[key]
    assert scn["topology"] == expected_t, (
        f"{key}: expected topology {expected_t!r}, got {scn['topology']!r}")
    rpo = scn["rpo_seconds"]
    rto = scn["rto_seconds"]
    assert rpo_lo <= rpo <= rpo_hi, (
        f"{key}: rpo_seconds {rpo} outside defensible band [{rpo_lo}, {rpo_hi}] "
        f"for {expected_t}")
    assert rto_lo <= rto <= rto_hi, (
        f"{key}: rto_seconds {rto} outside defensible band [{rto_lo}, {rto_hi}] "
        f"for {expected_t}")
    missing = required - _factors(scn)
    assert not missing, f"{key}: missing required factors {missing}"
