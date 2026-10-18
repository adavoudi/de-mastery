"""Stage 5 — keyed state + timers (process functions).

A KeyedProcessFunction has per-key ValueState and can register event-time timers
that fire when the watermark passes their timestamp, in (ts, key) order. We build
a session-window-by-timeout and a counter to exercise state + timers. Oracle:
independent Python recomputation of the expected session ends / counts.
"""


def _wm_stream(submission, events, delay=0):
    return submission.assign_watermarks(events, delay)


def test_timer_service_fires_in_order(submission):
    ts = submission.TimerService()
    ts.register("b", 20)
    ts.register("a", 10)
    ts.register("a", 20)
    fired = ts.advance(15)
    assert fired == [("a", 10)]            # only ts<=15, (ts,key) order
    fired2 = ts.advance(25)
    assert fired2 == [("a", 20), ("b", 20)]
    assert ts.advance(100) == []           # all consumed


def test_value_state_per_key(submission):
    st = submission.ValueState(default=0)
    assert st.value("a") == 0
    st.update("a", 5)
    st.update("b", 7)
    assert st.value("a") == 5 and st.value("b") == 7
    st.clear("a")
    assert st.value("a") == 0


def test_session_timeout(submission):
    """Session closes GAP after the last event of a key (fired by watermark)."""
    class Session(submission.ProcessFunction):
        GAP = 10

        def process_element(self, event, out):
            k = self._current_key
            self.state.update(k, event.ts)
            self.timers.register(k, event.ts + self.GAP)

        def on_timer(self, key, ts, out):
            last = self.state.value(key)
            if last is not None and ts == last + self.GAP:
                out(("end", key, last))
                self.state.clear(key)

    env = submission.Environment()
    E = submission.Event
    events = [E(key="a", value=1, ts=0), E(key="a", value=1, ts=3),
              E(key="a", value=1, ts=5),
              E(key="b", value=1, ts=20), E(key="b", value=1, ts=22),
              E(key="a", value=1, ts=40)]
    stream = _wm_stream(submission, events, delay=0)
    pr = env.source(stream).key_by(lambda e: e.key).process(Session())
    out = submission.run_process(pr)
    # a's first session ends at 5 (closed once wm passes 15 via b@20); b ends at
    # 22 (closed once wm passes 32 via a@40). a's last session (40) stays open.
    assert ("end", "a", 5) in out
    assert ("end", "b", 22) in out
    assert ("end", "a", 40) not in out


def test_counter_emits_on_timer(submission):
    class Counter(submission.ProcessFunction):
        def process_element(self, event, out):
            k = self._current_key
            self.state.update(k, (self.state.value(k) or 0) + 1)
            self.timers.register(k, 1000)      # flush timer

        def on_timer(self, key, ts, out):
            out((key, self.state.value(key)))

    env = submission.Environment()
    E = submission.Event
    events = [E(key="a", value=1, ts=1), E(key="a", value=1, ts=2),
              E(key="b", value=1, ts=3)]
    stream = _wm_stream(submission, events, delay=0) + [submission.Watermark(1000)]
    pr = env.source(stream).key_by(lambda e: e.key).process(Counter())
    out = dict(submission.run_process(pr))
    assert out == {"a": 2, "b": 1}


def test_state_snapshot_restore_roundtrip(submission):
    st = submission.ValueState()
    st.update("a", 1)
    st.update("b", 2)
    snap = st.snapshot()
    st.update("a", 99)
    st.restore(snap)
    assert st.value("a") == 1 and st.value("b") == 2
    # snapshot is a copy, not a live view
    snap["a"] = 123
    assert st.value("a") == 1
