"""Stage 3 — event time + watermarks (min across inputs).

Watermarks are monotone non-decreasing and generated as max_ts - delay. A
two-input operator's output watermark is the MIN of its inputs (it may not
advance past its slowest input). Oracle: independent recomputation in the test.
"""

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


def test_watermark_min(submission):
    assert submission.watermark_min([10, 20, 5]) == 5
    assert submission.watermark_min([7]) == 7


def test_bounded_out_of_orderness(submission):
    gen = submission.BoundedOutOfOrderness(delay=3)
    assert gen.current_watermark() is None  # nothing seen yet
    gen.on_event(10)
    assert gen.current_watermark() == 7
    gen.on_event(8)                           # out of order, older
    assert gen.current_watermark() == 7       # must NOT regress
    gen.on_event(20)
    assert gen.current_watermark() == 17


def test_assign_watermarks_monotone(submission):
    evs = [submission.Event(key="a", value=1, ts=t)
           for t in [10, 20, 15, 40, 35, 60]]
    out = submission.assign_watermarks(evs, delay=5)
    wms = [x.t for x in out if isinstance(x, submission.Watermark)]
    assert wms == sorted(wms)                 # non-decreasing
    assert all(wms[i] < wms[i + 1] for i in range(len(wms) - 1))  # strictly adv.
    # final watermark = max_ts - delay
    assert wms[-1] == 60 - 5


def test_assign_watermarks_interleaved_with_events(submission):
    evs = [submission.Event(key="a", value=1, ts=t) for t in [10, 20]]
    out = submission.assign_watermarks(evs, delay=0)
    # events preserved in order, each followed by its (advancing) watermark
    kinds = [(type(x).__name__, getattr(x, "t", getattr(x, "ts", None)))
             for x in out]
    assert kinds == [("Event", 10), ("Watermark", 10),
                     ("Event", 20), ("Watermark", 20)]


def test_merge_min_across_inputs(submission):
    s1 = [submission.Watermark(10), submission.Watermark(30),
          submission.Watermark(50)]
    s2 = [submission.Watermark(20), submission.Watermark(25),
          submission.Watermark(40)]
    merged = submission.merge_watermarks([s1, s2])
    wms = [x.t for x in merged if isinstance(x, submission.Watermark)]
    # Inputs are read round-robin; the combined watermark is MIN across inputs
    # and advances only when the min advances:
    #   r1: s1=10 (s2 unknown, no emit), s2=20 -> min 10
    #   r2: s1=30 -> min(30,20)=20,  s2=25 -> min(30,25)=25
    #   r3: s1=50 -> min(50,25)=25 (no advance), s2=40 -> min(50,40)=40
    assert wms == [10, 20, 25, 40]
    assert wms == sorted(wms)


def test_merge_blocks_on_silent_input(submission):
    """If one input never advances its watermark, the combined watermark is
    pinned there (the slow-input stall that idleness is meant to fix)."""
    s1 = [submission.Watermark(100), submission.Watermark(200)]
    s2 = [submission.Watermark(5)]
    merged = submission.merge_watermarks([s1, s2])
    wms = [x.t for x in merged if isinstance(x, submission.Watermark)]
    assert max(wms) == 5


@settings(max_examples=40, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(tss=st.lists(st.integers(min_value=0, max_value=1000),
                    min_size=1, max_size=30),
       delay=st.integers(min_value=0, max_value=50))
def test_property_monotone_and_formula(submission, tss, delay):
    evs = [submission.Event(key="a", value=1, ts=t) for t in tss]
    out = submission.assign_watermarks(evs, delay)
    wms = [x.t for x in out if isinstance(x, submission.Watermark)]
    assert wms == sorted(wms)
    if wms:
        assert wms[-1] == max(tss) - delay
        # every watermark equals (running max so far) - delay for some prefix
        assert all(w <= max(tss) - delay for w in wms)
