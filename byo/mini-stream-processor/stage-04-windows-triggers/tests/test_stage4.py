"""Stage 4 — tumbling/sliding windows with watermark triggers.

A window fires when the watermark passes its end. Late events (beyond allowed
lateness) are dropped to a side output. Oracle: an independent Python grouping of
events into windows and aggregating (G6). The streaming result after all
watermarks must equal this batch grouping.
"""

from collections import defaultdict


def _wm_stream(submission, triples, delay=0):
    evs = [submission.Event(key=k, value=v, ts=t) for k, v, t in triples]
    return submission.assign_watermarks(evs, delay)


def _tumbling_oracle(triples, size):
    acc = defaultdict(list)
    for k, v, t in triples:
        start = (t // size) * size
        acc[(k, start, start + size)].append(v)
    return {kw: sum(vs) for kw, vs in acc.items()}


def test_tumbling_assign(submission):
    tw = submission.TumblingWindows(10)
    assert [(w.start, w.end) for w in tw.assign(0)] == [(0, 10)]
    assert [(w.start, w.end) for w in tw.assign(9)] == [(0, 10)]
    assert [(w.start, w.end) for w in tw.assign(10)] == [(10, 20)]
    assert [(w.start, w.end) for w in tw.assign(25)] == [(20, 30)]


def test_sliding_assign(submission):
    sw = submission.SlidingWindows(10, 5)
    got = [(w.start, w.end) for w in sw.assign(12)]
    assert got == [(5, 15), (10, 20)]
    # Flink assigns a window for every slide step whose [start, start+size)
    # contains ts, including a window that starts before 0.
    got0 = [(w.start, w.end) for w in sw.assign(0)]
    assert got0 == [(-5, 5), (0, 10)]
    got7 = [(w.start, w.end) for w in sw.assign(7)]
    assert got7 == [(0, 10), (5, 15)]


def test_tumbling_fires_on_watermark_matches_oracle(submission):
    env = submission.Environment()
    # events in event-time order (no out-of-order lateness), delay 0
    triples = [("b", 9, 3), ("a", 1, 0), ("a", 2, 5), ("a", 3, 10),
               ("a", 4, 12), ("a", 5, 25)]
    triples = sorted(triples, key=lambda x: x[2])
    stream = _wm_stream(submission, triples, delay=0)
    ws = env.source(stream).key_by(lambda e: e.key).window(
        submission.TumblingWindows(10))
    fired = submission.run_windows(ws.aggregate(sum))
    got = {(k, w.start, w.end): r for k, w, r in fired}
    oracle = _tumbling_oracle(triples, 10)
    # windows whose end <= final watermark must have fired and match the oracle
    final_wm = max(t for _, _, t in triples)
    expected = {kw: v for kw, v in oracle.items() if kw[2] <= final_wm}
    assert got == expected


def test_window_fire_only_after_watermark_passes_end(submission):
    env = submission.Environment()
    # one event at ts=5 in window [0,10); watermark never reaches 10 => no fire
    triples = [("a", 1, 5)]
    stream = _wm_stream(submission, triples, delay=0)
    ws = env.source(stream).key_by(lambda e: e.key).window(
        submission.TumblingWindows(10))
    fired = submission.run_windows(ws.aggregate(sum))
    assert list(fired) == []


def test_late_event_dropped_to_side_output(submission):
    env = submission.Environment()
    # Build an explicit stream: event ts=1 (win [0,10)), then watermark 10
    # (fires + closes the window), then a LATE event ts=2 (window already closed).
    E, W = submission.Event, submission.Watermark
    stream = [E(key="a", value=1, ts=1), W(10),
              E(key="a", value=99, ts=2), W(11)]
    ws = env.source(stream).key_by(lambda e: e.key).window(
        submission.TumblingWindows(10))
    res = submission.run_windows(ws.aggregate(sum, allowed_lateness=0))
    got = {(k, w.start, w.end): r for k, w, r in res}
    assert got == {("a", 0, 10): 1}          # 99 NOT included
    assert [e.value for e in res.late] == [99]


def test_allowed_lateness_admits_within_grace(submission):
    env = submission.Environment()
    E, W = submission.Event, submission.Watermark
    # window [0,10), wm=10 then wm=14; a late event ts=2 with lateness 10 is
    # still admitted (10 + 10 > 14), so it is NOT dropped.
    stream = [E(key="a", value=1, ts=1), W(10),
              E(key="a", value=5, ts=2), W(14)]
    ws = env.source(stream).key_by(lambda e: e.key).window(
        submission.TumblingWindows(10))
    res = submission.run_windows(ws.aggregate(sum, allowed_lateness=10))
    assert [e.value for e in res.late] == []  # admitted, not late-dropped
