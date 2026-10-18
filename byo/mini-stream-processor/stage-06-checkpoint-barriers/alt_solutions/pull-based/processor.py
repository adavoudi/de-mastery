"""mini-stream-processor — ALT cumulative reference (genuinely different).

Same public API as processor_full.py, but implemented differently:
- Stage 1 execution is PULL-based: each operator is a generator that pulls from
  its upstream iterator, rather than push callbacks.
- Keyed reduce (stage 2) buckets records then uses functools.reduce per bucket,
  rather than an incremental fold.
- Watermarks (stage 3) computed with itertools.accumulate for the running max.
- Windows (stage 4) assign with ts-(ts%size) and fire on watermark scan.
All 8 stages pass. Pure stdlib.
"""

from __future__ import annotations

import functools
import hashlib
import itertools
from dataclasses import dataclass
from typing import Any, Callable


# ===========================================================================
# Records and markers
# ===========================================================================
@dataclass(frozen=True)
class Event:
    key: Any = None
    value: Any = None
    ts: int | None = None


@dataclass(frozen=True)
class Watermark:
    t: int


@dataclass(frozen=True)
class Barrier:
    id: int


# ===========================================================================
# Stage 1 — operators (pull-based generators)
# ===========================================================================
class Operator:
    def process(self, elem, out):
        out(elem)

    def open(self):
        pass

    def close(self, out):
        pass


class Source(Operator):
    def __init__(self, records):
        self.records = list(records)

    def run(self, out):
        for r in self.records:
            out(r)


class MapOp(Operator):
    def __init__(self, fn):
        self.fn = fn

    def apply(self, elem):
        if isinstance(elem, (Watermark, Barrier)):
            return elem
        return self.fn(elem)

    def process(self, elem, out):
        out(self.apply(elem))


class FilterOp(Operator):
    def __init__(self, fn):
        self.fn = fn

    def keep(self, elem):
        return isinstance(elem, (Watermark, Barrier)) or self.fn(elem)

    def process(self, elem, out):
        if self.keep(elem):
            out(elem)


class SinkOp(Operator):
    def __init__(self):
        self.collected = []

    def process(self, elem, out):
        if not isinstance(elem, (Watermark, Barrier)):
            self.collected.append(elem)


class Stream:
    def __init__(self, env, ops):
        self.env = env
        self.ops = ops

    def map(self, fn):
        return Stream(self.env, self.ops + [MapOp(fn)])

    def filter(self, fn):
        return Stream(self.env, self.ops + [FilterOp(fn)])

    def key_by(self, key_fn):
        return KeyedStream(self.env, self.ops, key_fn)

    def sink(self):
        return Stream(self.env, self.ops + [SinkOp()])


class Environment:
    def source(self, records):
        return Stream(self, [Source(records)])


def _pull(ops):
    """Pull-based pipeline generator; yields terminal elements."""
    stream = iter(ops[0].records)
    for op in ops[1:]:
        if isinstance(op, MapOp):
            def mapped(src, o=op):
                for e in src:
                    yield o.apply(e)
            stream = mapped(stream)
        elif isinstance(op, FilterOp):
            def filtered(src, o=op):
                for e in src:
                    if o.keep(e):
                        yield e
            stream = filtered(stream)
        elif isinstance(op, SinkOp):
            def tap(src, s=op):
                for e in src:
                    if not isinstance(e, (Watermark, Barrier)):
                        s.collected.append(e)
                    yield e
            stream = tap(stream)
    return stream


def _drive_chain(ops):
    """Compat shim: materialize terminal output + trailing sink."""
    sink = ops[-1] if isinstance(ops[-1], SinkOp) else None
    terminal = list(_pull(ops))
    return sink, terminal


def collect(stream: Stream) -> list:
    ops = stream.ops
    sink = ops[-1] if isinstance(ops[-1], SinkOp) else None
    terminal = list(_pull(ops))
    if sink is not None:
        return list(sink.collected)
    return [e for e in terminal if not isinstance(e, (Watermark, Barrier))]


# ===========================================================================
# Stage 2 — keyed partitioning
# ===========================================================================
def stable_hash(value) -> int:
    return int.from_bytes(
        hashlib.blake2b(repr(value).encode(), digest_size=8).digest(), "big")


def partition_of(key, num_partitions: int) -> int:
    return stable_hash(key) % num_partitions


class KeyedStream:
    def __init__(self, env, ops, key_fn):
        self.env = env
        self.ops = ops
        self.key_fn = key_fn

    def map(self, fn):
        return Stream(self.env, self.ops + [MapOp(fn)])

    def reduce(self, reduce_fn, parallelism: int = 1):
        return KeyedResult(self.env, self.ops, self.key_fn, reduce_fn,
                           parallelism)

    def process(self, process_fn, parallelism: int = 1):
        return KeyedProcessResult(self.env, self.ops, self.key_fn, process_fn,
                                  parallelism)

    def window(self, assigner):
        return WindowedStream(self.env, self.ops, self.key_fn, assigner)


@dataclass
class KeyedResult:
    env: Any
    ops: list
    key_fn: Callable
    reduce_fn: Callable
    parallelism: int = 1


def run_parallel(keyed: KeyedResult) -> dict:
    _, out = _drive_chain(keyed.ops)
    events = [e for e in out if isinstance(e, Event)]
    buckets: dict = {}
    for ev in events:
        k = keyed.key_fn(ev)
        p = partition_of(k, keyed.parallelism)
        buckets.setdefault((p, k), []).append(ev.value)
    merged = {}
    for (p, k), vals in buckets.items():
        merged[k] = functools.reduce(keyed.reduce_fn, vals)
    return merged


def worker_assignment(keys, num_partitions: int) -> dict:
    return {k: partition_of(k, num_partitions) for k in keys}


# ===========================================================================
# Stage 3 — event time + watermarks
# ===========================================================================
def watermark_min(input_watermarks) -> int:
    return min(input_watermarks)


class BoundedOutOfOrderness:
    def __init__(self, delay: int):
        self.delay = delay
        self.max_ts = None
        self.last_wm = None

    def on_event(self, ts: int):
        self.max_ts = ts if self.max_ts is None else max(self.max_ts, ts)

    def current_watermark(self):
        if self.max_ts is None:
            return None
        wm = self.max_ts - self.delay
        self.last_wm = wm if self.last_wm is None else max(self.last_wm, wm)
        return self.last_wm


def assign_watermarks(events, delay: int):
    tss = [e.ts for e in events]
    running_max = list(itertools.accumulate(tss, max)) if tss else []
    out = []
    last = None
    for ev, mx in zip(events, running_max):
        out.append(ev)
        wm = mx - delay
        if last is None or wm > last:
            out.append(Watermark(wm))
            last = wm
    return out


def merge_watermarks(streams):
    merged = []
    current = [None] * len(streams)
    emitted = None
    iters = [iter(s) for s in streams]
    done = [False] * len(streams)
    while not all(done):
        for i, it in enumerate(iters):
            if done[i]:
                continue
            try:
                elem = next(it)
            except StopIteration:
                done[i] = True
                continue
            if isinstance(elem, Watermark):
                current[i] = elem.t
                if all(v is not None for v in current):
                    combined = watermark_min(current)
                    if emitted is None or combined > emitted:
                        emitted = combined
                        merged.append(Watermark(combined))
            else:
                merged.append(elem)
    return merged


# ===========================================================================
# Stage 4 — windows + triggers
# ===========================================================================
@dataclass(frozen=True)
class Window:
    start: int
    end: int


class TumblingWindows:
    def __init__(self, size: int):
        self.size = size

    def assign(self, ts: int):
        start = ts - (ts % self.size)
        return [Window(start, start + self.size)]


class SlidingWindows:
    def __init__(self, size: int, slide: int):
        self.size = size
        self.slide = slide

    def assign(self, ts: int):
        wins = []
        s = ts - (ts % self.slide)
        while s > ts - self.size:
            if s <= ts < s + self.size:
                wins.append(Window(s, s + self.size))
            s -= self.slide
        return sorted(wins, key=lambda w: w.start)


@dataclass
class WindowedStream:
    env: Any
    ops: list
    key_fn: Callable
    assigner: Any

    def aggregate(self, agg_fn, allowed_lateness: int = 0):
        return WindowResult(self.env, self.ops, self.key_fn, self.assigner,
                            agg_fn, allowed_lateness)


@dataclass
class WindowResult:
    env: Any
    ops: list
    key_fn: Callable
    assigner: Any
    agg_fn: Callable
    allowed_lateness: int = 0


class Fired(list):
    late: list = []


def run_windows(wr: WindowResult):
    _, out = _drive_chain(wr.ops)
    panes: dict = {}
    fired = []
    late = []
    wm = None
    for elem in out:
        if isinstance(elem, Watermark):
            wm = elem.t if wm is None else max(wm, elem.t)
            ready = [kw for kw in list(panes) if kw[1].end <= wm]
            for kw in sorted(ready, key=lambda x: (x[1].start, repr(x[0]))):
                fired.append((kw[0], kw[1], wr.agg_fn(panes.pop(kw))))
        elif isinstance(elem, Event):
            windows = wr.assigner.assign(elem.ts)
            max_end = max((w.end for w in windows), default=0)
            if wm is not None and max_end + wr.allowed_lateness <= wm:
                late.append(elem)
                continue
            k = wr.key_fn(elem)
            for w in windows:
                panes.setdefault((k, w), []).append(elem.value)
    res = Fired(fired)
    res.late = late
    return res


# ===========================================================================
# Stage 5 — keyed state + timers
# ===========================================================================
class ValueState:
    def __init__(self, default=None):
        self.default = default
        self._v = {}

    def value(self, key):
        return self._v.get(key, self.default)

    def update(self, key, v):
        self._v[key] = v

    def clear(self, key):
        self._v.pop(key, None)

    def snapshot(self):
        return dict(self._v)

    def restore(self, state):
        self._v = dict(state)


class TimerService:
    def __init__(self):
        self._timers = set()

    def register(self, key, ts):
        self._timers.add((key, ts))

    def advance(self, watermark):
        ready = sorted((t for t in self._timers if t[1] <= watermark),
                       key=lambda t: (t[1], repr(t[0])))
        self._timers.difference_update(ready)
        return ready


class ProcessFunction:
    def open(self, state: ValueState, timers: TimerService):
        self.state = state
        self.timers = timers

    def process_element(self, event: Event, out):
        raise NotImplementedError

    def on_timer(self, key, ts, out):
        pass


@dataclass
class KeyedProcessResult:
    env: Any
    ops: list
    key_fn: Callable
    process_fn: ProcessFunction
    parallelism: int = 1


def run_process(kpr: KeyedProcessResult) -> list:
    _, stream = _drive_chain(kpr.ops)
    state = ValueState()
    timers = TimerService()
    pf = kpr.process_fn
    pf.open(state, timers)
    out_list = []
    out = out_list.append
    wm = None
    for elem in stream:
        if isinstance(elem, Watermark):
            wm = elem.t if wm is None else max(wm, elem.t)
            for (k, ts) in timers.advance(wm):
                pf._current_key = k
                pf.on_timer(k, ts, out)
        elif isinstance(elem, Event):
            pf._current_key = kpr.key_fn(elem)
            pf.process_element(elem, out)
    return out_list


# ===========================================================================
# Stage 6 — checkpoint barriers
# ===========================================================================
class StatefulSumOperator(Operator):
    def __init__(self, key_fn):
        self.key_fn = key_fn
        self.state = {}
        self.checkpoints = {}

    def process(self, elem, out):
        if isinstance(elem, Barrier):
            self.checkpoints[elem.id] = dict(self.state)
            out(elem)
        elif isinstance(elem, Watermark):
            out(elem)
        else:
            k = self.key_fn(elem)
            self.state[k] = self.state.get(k, 0) + elem.value
            out(Event(key=k, value=self.state[k], ts=elem.ts))


@dataclass
class Checkpoint:
    id: int
    operator_states: dict
    source_offset: int


def inject_barriers(records, every: int, start_id: int = 1):
    out = []
    seen = 0
    cid = start_id
    for r in records:
        out.append(r)
        if isinstance(r, Event):
            seen += 1
            if seen % every == 0:
                out.append(Barrier(cid))
                cid += 1
    return out


def run_with_checkpoints(records, key_fn, barrier_every: int):
    marked = inject_barriers(records, barrier_every)
    op = StatefulSumOperator(key_fn)
    outputs = []
    checkpoints = {}
    consumed = 0
    for elem in marked:
        if isinstance(elem, Event):
            consumed += 1
            op.process(elem, lambda e: outputs.append((e.key, e.value)))
        elif isinstance(elem, Barrier):
            op.process(elem, lambda e: None)
            checkpoints[elem.id] = Checkpoint(
                id=elem.id,
                operator_states={"sum": dict(op.checkpoints[elem.id])},
                source_offset=consumed,
            )
    return outputs, checkpoints


# ===========================================================================
# Stage 7 — recovery
# ===========================================================================
class ReplayableSource:
    def __init__(self, records):
        self.records = list(records)

    def replay_from(self, offset: int):
        return self.records[offset:]


def run_with_recovery(records, key_fn, barrier_every: int, crash_after: int):
    source = ReplayableSource(records)
    marked = inject_barriers(records, barrier_every)
    op = StatefulSumOperator(key_fn)
    checkpoints = {}
    consumed = 0
    last_completed = None
    crashed = False
    for elem in marked:
        if isinstance(elem, Event):
            if consumed >= crash_after:
                crashed = True
                break
            consumed += 1
            op.process(elem, lambda e: None)
        elif isinstance(elem, Barrier):
            op.process(elem, lambda e: None)
            checkpoints[elem.id] = Checkpoint(
                id=elem.id,
                operator_states={"sum": dict(op.checkpoints[elem.id])},
                source_offset=consumed,
            )
            last_completed = elem.id
    if not crashed:
        return dict(op.state)
    op2 = StatefulSumOperator(key_fn)
    if last_completed is not None:
        cp = checkpoints[last_completed]
        op2.state = dict(cp.operator_states["sum"])
        off = cp.source_offset
    else:
        off = 0
    for ev in source.replay_from(off):
        if isinstance(ev, Event):
            op2.process(ev, lambda e: None)
    return dict(op2.state)


# ===========================================================================
# Stage 8 — transactional (2PC) sink
# ===========================================================================
class TransactionalSink:
    def __init__(self):
        self.open_txn = []
        self.prepared = {}
        self.committed = []
        self._committed_cids = set()

    def write(self, record):
        self.open_txn.append(record)

    def pre_commit(self, cid: int):
        self.prepared[cid] = list(self.open_txn)
        self.open_txn = []

    def commit(self, cid: int):
        if cid in self.prepared and cid not in self._committed_cids:
            self.committed.extend(self.prepared[cid])
            self._committed_cids.add(cid)

    def abort(self, cid: int):
        self.prepared.pop(cid, None)

    def abort_open(self):
        self.open_txn = []


def run_exactly_once_sink(records, barrier_every: int, crash_after: int,
                          commit_lag: int = 0):
    marked = inject_barriers(records, barrier_every)
    source = ReplayableSource(records)
    sink = TransactionalSink()
    offsets = {}
    completed = set()
    pending = []
    consumed = 0

    def deliver(flush=False):
        while pending and (flush or len(pending) > commit_lag):
            due = pending.pop(0)
            sink.commit(due)
            completed.add(due)

    crashed = False
    for elem in marked:
        if isinstance(elem, Event):
            if consumed >= crash_after:
                crashed = True
                break
            consumed += 1
            sink.write((elem.key, elem.value, elem.ts))
        elif isinstance(elem, Barrier):
            sink.pre_commit(elem.id)
            offsets[elem.id] = consumed
            pending.append(elem.id)
            deliver()

    if not crashed:
        final_cid = (max(offsets) if offsets else 0) + 1
        sink.pre_commit(final_cid)
        offsets[final_cid] = consumed
        pending.append(final_cid)
        deliver(flush=True)
        return sorted(sink.committed)

    sink.abort_open()
    for cid in list(sink.prepared):
        if cid not in completed:
            sink.abort(cid)
    last_completed = max(completed) if completed else None
    off = offsets[last_completed] if last_completed is not None else 0
    visible = list(sink.committed)
    sink2 = TransactionalSink()
    for ev in source.replay_from(off):
        if isinstance(ev, Event):
            sink2.write((ev.key, ev.value, ev.ts))
    rcid = (last_completed or 0) + 1000
    sink2.pre_commit(rcid)
    sink2.commit(rcid)
    visible.extend(sink2.committed)
    return sorted(visible)
