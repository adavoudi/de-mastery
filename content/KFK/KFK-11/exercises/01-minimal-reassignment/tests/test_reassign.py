"""Tests for KFK-11 / 01-minimal-reassignment.

Pure-planner layer (no broker, so G1 is reachable on a bare machine):
  * validity properties (rf, distinct, subset, rack spread),
  * MINIMAL-movement property vs an independent oracle (no still-valid current
    replica is ever moved).
Live layer (native multi-broker cluster, no Docker):
  * apply the plan with kafka-reassign-partitions.sh and verify the placement +
    rack constraint on the real cluster.
"""

import uuid

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st


def _uid():
    return uuid.uuid4().hex[:8]


# --------------------------------------------------------------------------
# oracle helpers (independent of the submission)
# --------------------------------------------------------------------------
def _n_racks(broker_set, racks):
    if not racks:
        return 0
    return len({racks[b] for b in broker_set if b in racks})


def _valid(plan, current, broker_set, rf, racks):
    bs = set(broker_set)
    n_racks = _n_racks(broker_set, racks)
    enforce = bool(racks) and rf <= n_racks and n_racks > 0
    for p, reps in plan.items():
        assert len(reps) == rf, f"p{p}: len {len(reps)} != rf {rf}"
        assert len(set(reps)) == rf, f"p{p}: duplicate brokers {reps}"
        assert set(reps) <= bs, f"p{p}: {reps} not subset of {broker_set}"
        if enforce:
            rk = [racks[b] for b in reps]
            assert len(set(rk)) == rf, f"p{p}: racks {rk} not all distinct"


def _no_needless_move(plan, current, broker_set, rf, racks):
    """A still-valid current replica must not be dropped: for each current
    replica that is in broker_set but absent from the plan, keeping it in place
    of some chosen non-current broker would have to break a rule — otherwise the
    planner moved more than necessary."""
    bs = set(broker_set)
    n_racks = _n_racks(broker_set, racks)
    enforce = bool(racks) and rf <= n_racks and n_racks > 0
    for p, reps in plan.items():
        chosen = set(reps)
        kept = [b for b in current[p] if b in chosen]
        # any current replica eligible but dropped?
        for b in current[p]:
            if b in chosen or b not in bs:
                continue
            # would adding b (replacing a NON-current chosen broker) keep validity?
            non_current_chosen = [c for c in reps if c not in current[p]]
            if not non_current_chosen:
                continue  # all chosen are current replicas; nothing needless
            if enforce:
                # b is retainable only if its rack isn't already covered by the
                # other kept-current replicas
                kept_racks = {racks[k] for k in kept if k != b}
                if racks[b] in kept_racks:
                    continue  # genuinely forced out by rack rule
            # b is in broker_set, rack-ok, yet dropped while a brand-new broker
            # was added => needless movement.
            raise AssertionError(
                f"p{p}: dropped reusable current replica {b} (plan={reps}, "
                f"current={current[p]})")


# --------------------------------------------------------------------------
# deterministic unit cases
# --------------------------------------------------------------------------
def test_keep_one_move_one(submission):
    plan = submission.plan_reassignment({0: [0, 1]}, [0, 2], rf=2)
    assert set(plan[0]) == {0, 2}          # keep 0, move 1->2
    assert 0 in plan[0]                     # the reusable replica stayed


def test_reorder_is_free_no_change_needed(submission):
    # current already valid and on the target set => keep as-is (no moves)
    plan = submission.plan_reassignment({0: [1, 2]}, [0, 1, 2], rf=2)
    assert set(plan[0]) == {1, 2}


def test_rack_distinct(submission):
    racks = {0: "a", 1: "a", 2: "b", 3: "c"}
    plan = submission.plan_reassignment({0: [0, 1]}, [0, 1, 2, 3], rf=3,
                                        racks=racks)
    rk = [racks[b] for b in plan[0]]
    assert len(set(rk)) == 3               # one replica per rack
    assert 0 in plan[0]                     # keep a rack-a replica (either 0 or 1)


def test_expand_fills_new_broker(submission):
    # add broker 2, rf grows 2->3
    plan = submission.plan_reassignment({0: [0, 1]}, [0, 1, 2], rf=3)
    assert set(plan[0]) == {0, 1, 2}        # keep both, add 2


# --------------------------------------------------------------------------
# property: validity + minimal movement over random inputs
# --------------------------------------------------------------------------
@settings(max_examples=120, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.data())
def test_plan_valid_and_minimal(submission, data):
    n_brokers = data.draw(st.integers(3, 6))
    broker_set = list(range(n_brokers))
    rf = data.draw(st.integers(1, n_brokers))
    n_parts = data.draw(st.integers(1, 6))
    use_racks = data.draw(st.booleans())
    racks = None
    if use_racks:
        rack_names = data.draw(st.lists(st.sampled_from(["a", "b", "c", "d"]),
                                        min_size=n_brokers, max_size=n_brokers))
        racks = {b: rack_names[b] for b in broker_set}
        # only enforce-able racks are interesting, but the planner must stay valid
        assume(_n_racks(broker_set, racks) >= 1)

    current = {}
    for p in range(n_parts):
        size = data.draw(st.integers(1, n_brokers))
        reps = data.draw(st.lists(st.integers(0, n_brokers + 1),
                                  min_size=size, max_size=size, unique=True))
        current[p] = reps

    plan = submission.plan_reassignment(current, broker_set, rf, racks=racks)
    _valid(plan, current, broker_set, rf, racks)
    _no_needless_move(plan, current, broker_set, rf, racks)


# --------------------------------------------------------------------------
# live cluster: apply the plan and verify placement
# --------------------------------------------------------------------------
def test_apply_on_cluster(kafka_cluster, submission):
    c = kafka_cluster(n_brokers=3)
    topic = f"re-{_uid()}"
    c.create_topic(topic, partitions=4, replication_factor=2)

    a = c.admin()
    md = a.list_topics(timeout=15)
    current = {pid: list(pm.replicas)
               for pid, pm in md.topics[topic].partitions.items()}

    # retarget onto brokers {1, 3} (drop broker 2), rf stays 2
    plan = submission.plan_reassignment(current, [1, 3], rf=2)
    for reps in plan.values():
        assert set(reps) <= {1, 3} and len(set(reps)) == 2

    assert submission.apply_reassignment(c, topic, plan) is True

    md2 = a.list_topics(timeout=15)
    after = {pid: sorted(pm.replicas)
             for pid, pm in md2.topics[topic].partitions.items()}
    for pid, reps in plan.items():
        assert after[pid] == sorted(reps), f"partition {pid}: {after[pid]} != {sorted(reps)}"
    # broker 2 no longer hosts any replica of this topic
    assert all(2 not in reps for reps in after.values())
