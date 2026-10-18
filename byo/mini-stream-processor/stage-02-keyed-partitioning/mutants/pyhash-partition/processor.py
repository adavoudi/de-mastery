"""mini-stream-processor — FULL cumulative reference (all 8 stages).

This is the source of truth. `_src/build_tree.py` stamps this file into every
stage's solution/ (it passes stages 1..N for all N — a legal cumulative
reference) and the challenge-level starter/ is a stubbed copy of the SAME public
API. Pure Python stdlib only: deterministic, no Flink, no JVM, no Docker.

Design: a push-based dataflow of operators. A record flows downstream element by
element; watermarks and checkpoint barriers are special markers that also flow
downstream, so the whole thing is one in-process simulation of a Flink job.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable


# ===========================================================================
# Records and markers (flow downstream through operators)
# ===========================================================================
@dataclass(frozen=True)
class Event:
    """A data record. ``ts`` is the event-time timestamp (ms); may be None in
    stages that do not use time."""
    key: Any = None
    value: Any = None
    ts: int | None = None


@dataclass(frozen=True)
class Watermark:
    """A watermark marker: event time has advanced to ``t`` (no more events with
    ts <= t are expected on this input)."""
    t: int


@dataclass(frozen=True)
class Barrier:
    """A checkpoint barrier (Chandy-Lamport / Flink ABS). ``id`` identifies the
    checkpoint it belongs to."""
    id: int


# ===========================================================================
# Stage 1 — operators + push-based execution
# ===========================================================================
class Operator:
    """Base push operator. ``process(elem, out)`` pushes 0+ elements to ``out``
    (a callable). Markers (Watermark/Barrier) are forwarded by default."""

    def process(self, elem, out):
        out(elem)

    def open(self):
        pass

    def close(self, out):
        pass


class Source(Operator):
    """Emits a fixed list of elements (Events and/or markers) in order."""

    def __init__(self, records):
        self.records = list(records)

    def run(self, out):
        for r in self.records:
            out(r)


class MapOp(Operator):
    def __init__(self, fn: Callable[[Any], Any]):
        self.fn = fn

    def process(self, elem, out):
        if isinstance(elem, (Watermark, Barrier)):
            out(elem)
        else:
            out(self.fn(elem))


class FilterOp(Operator):
    def __init__(self, fn: Callable[[Any], bool]):
        self.fn = fn

    def process(self, elem, out):
        if isinstance(elem, (Watermark, Barrier)):
            out(elem)
        elif self.fn(elem):
            out(elem)


class SinkOp(Operator):
    """Collects data events into ``self.out`` (markers are dropped)."""

    def __init__(self):
        self.collected = []

    def process(self, elem, out):
        if not isinstance(elem, (Watermark, Barrier)):
            self.collected.append(elem)


class Stream:
    """A fluent builder of a linear operator chain starting at a Source."""

    def __init__(self, env, ops):
        self.env = env
        self.ops = ops  # list of Operator (first is Source)

    def map(self, fn) -> "Stream":
        return Stream(self.env, self.ops + [MapOp(fn)])

    def filter(self, fn) -> "Stream":
        return Stream(self.env, self.ops + [FilterOp(fn)])

    def key_by(self, key_fn) -> "KeyedStream":
        return KeyedStream(self.env, self.ops, key_fn)

    def sink(self) -> "Stream":
        return Stream(self.env, self.ops + [SinkOp()])


class Environment:
    """Entry point. ``env.source([...])`` starts a stream."""

    def source(self, records) -> Stream:
        return Stream(self, [Source(records)])


def _drive_chain(ops):
    """Push every source record through the operator chain. Returns the terminal
    SinkOp (if any) and the full output list of the last operator."""
    sink = ops[-1] if isinstance(ops[-1], SinkOp) else None
    terminal_out = []

    def make_stage(i):
        if i == len(ops):
            return lambda e: terminal_out.append(e)
        op = ops[i]
        downstream = make_stage(i + 1)

        def feed(e):
            op.process(e, downstream)
        return feed

    first_proc = make_stage(1)  # ops[0] is the Source
    for op in ops[1:]:
        op.open()
    src = ops[0]
    src.run(first_proc)
    for op in ops[1:]:
        op.close(first_proc)
    return sink, terminal_out


def collect(stream: Stream) -> list:
    """Run a linear stream and return the Sink's collected data events (values).

    If the chain ends in a Sink, return its collected events; otherwise return
    the raw terminal output (data events only)."""
    ops = stream.ops
    sink, terminal_out = _drive_chain(ops)
    if sink is not None:
        return list(sink.collected)
    return [e for e in terminal_out if not isinstance(e, (Watermark, Barrier))]


# ===========================================================================
# Stage 2 — keyed partitioning across workers
# ===========================================================================
def stable_hash(value) -> int:
    """Deterministic hash across processes/runs (Python's hash() is salted)."""
    return int.from_bytes(
        hashlib.blake2b(repr(value).encode(), digest_size=8).digest(), "big")


def partition_of(key, num_partitions: int) -> int:
    """Which partition (worker) a key is routed to. Flink: murmurHash % n."""
    return hash(key) % num_partitions


class KeyedStream:
    def __init__(self, env, ops, key_fn):
        self.env = env
        self.ops = ops
        self.key_fn = key_fn

    def map(self, fn) -> "Stream":
        return Stream(self.env, self.ops + [MapOp(fn)])

    def reduce(self, reduce_fn, parallelism: int = 1) -> "KeyedResult":
        return KeyedResult(self.env, self.ops, self.key_fn, reduce_fn,
                           parallelism)

    def process(self, process_fn, parallelism: int = 1) -> "KeyedProcessResult":
        return KeyedProcessResult(self.env, self.ops, self.key_fn, process_fn,
                                  parallelism)

    def window(self, assigner) -> "WindowedStream":
        return WindowedStream(self.env, self.ops, self.key_fn, assigner)


@dataclass
class KeyedResult:
    env: Any
    ops: list
    key_fn: Callable
    reduce_fn: Callable
    parallelism: int = 1


def run_parallel(keyed: KeyedResult) -> dict:
    """Keyed reduce across ``parallelism`` workers. Returns {key: reduced_value}.

    Each key is routed by ``partition_of`` to exactly one worker; the result is
    independent of the number of workers (the key-group invariant)."""
    _, out = _drive_chain(keyed.ops)
    events = [e for e in out if isinstance(e, Event)]
    workers = {p: {} for p in range(keyed.parallelism)}
    for ev in events:
        k = keyed.key_fn(ev)
        p = partition_of(k, keyed.parallelism)
        state = workers[p]
        if k in state:
            state[k] = keyed.reduce_fn(state[k], ev.value)
        else:
            state[k] = ev.value
    merged = {}
    for p in range(keyed.parallelism):
        for k, v in workers[p].items():
            merged[k] = v  # each key lives on exactly one worker; no conflict
    return merged


def worker_assignment(keys, num_partitions: int) -> dict:
    """{key: worker_index} — exposes the routing for tests."""
    return {k: partition_of(k, num_partitions) for k in keys}


# ===========================================================================
# Stage 3 — event time + watermarks (min across inputs)
# ===========================================================================
def watermark_min(input_watermarks) -> int:
    """An operator's output watermark is the MIN of its input watermarks (a
    downstream op must not advance past the slowest input)."""
    return min(input_watermarks)


class BoundedOutOfOrderness:
    """Watermark generator: wm = max_ts_seen - delay (monotone non-decreasing)."""

    def __init__(self, delay: int):
        self.delay = delay
        self.max_ts = None
        self.last_wm = None

    def on_event(self, ts: int):
        if self.max_ts is None or ts > self.max_ts:
            self.max_ts = ts

    def current_watermark(self) -> int | None:
        if self.max_ts is None:
            return None
        wm = self.max_ts - self.delay
        if self.last_wm is None or wm > self.last_wm:
            self.last_wm = wm
        return self.last_wm


def assign_watermarks(events, delay: int):
    """Interleave Watermarks into an event stream using
    BoundedOutOfOrderness(delay). A watermark is emitted (only when it advances)
    after each event. Returns a list of Events and Watermarks."""
    gen = BoundedOutOfOrderness(delay)
    out = []
    last = None
    for ev in events:
        out.append(ev)
        gen.on_event(ev.ts)
        wm = gen.current_watermark()
        if wm is not None and (last is None or wm > last):
            out.append(Watermark(wm))
            last = wm
    return out


def merge_watermarks(streams):
    """Merge N watermark-carrying input streams the way a two-input operator
    does: track each input's latest watermark, and the combined watermark is the
    MIN across inputs. Returns the merged (Event|Watermark) list in a
    deterministic order (round-robin by original index), with the combined
    watermark recomputed as min-across-inputs after each input watermark."""
    # Flatten preserving per-stream order via a merge by position.
    merged = []
    current = {i: None for i in range(len(streams))}
    emitted_wm = None
    iters = [iter(s) for s in streams]
    exhausted = [False] * len(streams)
    while not all(exhausted):
        for i, it in enumerate(iters):
            if exhausted[i]:
                continue
            try:
                elem = next(it)
            except StopIteration:
                exhausted[i] = True
                continue
            if isinstance(elem, Watermark):
                current[i] = elem.t
                have = [v for v in current.values() if v is not None]
                if len(have) == len(streams):
                    combined = watermark_min(have)
                    if emitted_wm is None or combined > emitted_wm:
                        emitted_wm = combined
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
    end: int  # exclusive


class TumblingWindows:
    def __init__(self, size: int):
        self.size = size

    def assign(self, ts: int) -> list[Window]:
        start = (ts // self.size) * self.size
        return [Window(start, start + self.size)]


class SlidingWindows:
    def __init__(self, size: int, slide: int):
        self.size = size
        self.slide = slide

    def assign(self, ts: int) -> list[Window]:
        # Flink: windows whose [start, start+size) contains ts.
        last_start = (ts // self.slide) * self.slide
        wins = []
        start = last_start
        while start > ts - self.size:
            if start <= ts < start + self.size:
                wins.append(Window(start, start + self.size))
            start -= self.slide
        return sorted(wins, key=lambda w: w.start)


@dataclass
class WindowedStream:
    env: Any
    ops: list
    key_fn: Callable
    assigner: Any

    def aggregate(self, agg_fn, allowed_lateness: int = 0) -> "WindowResult":
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


def run_windows(wr: WindowResult):
    """Event-time windowed aggregation with watermark-driven triggers.

    - Each event is assigned to window(s) by the assigner; dropped if it is late
      beyond ``allowed_lateness`` (ts + lateness < current watermark end).
    - A window FIRES (emits (key, Window, agg)) when the watermark passes its end.
    - Returns a list of (key, Window, result) in fire order, plus a side list of
      dropped-late events via the ``.late`` attribute of the returned Fired.
    """
    _, out = _drive_chain(wr.ops)
    # state: (key, Window) -> list of values
    panes: dict = {}
    fired = []
    late = []
    watermark = None

    def fire_ready():
        ready = [kw for kw in panes
                 if watermark is not None and kw[1].end <= watermark]
        for kw in sorted(ready, key=lambda x: (x[1].start, repr(x[0]))):
            vals = panes.pop(kw)
            fired.append((kw[0], kw[1], wr.agg_fn(vals)))

    for elem in out:
        if isinstance(elem, Watermark):
            watermark = elem.t if watermark is None else max(watermark, elem.t)
            fire_ready()
        elif isinstance(elem, Event):
            k = wr.key_fn(elem)
            # late check: event whose max window end is already below the wm
            max_end = max((w.end for w in wr.assigner.assign(elem.ts)),
                          default=0)
            if watermark is not None and \
                    max_end + wr.allowed_lateness <= watermark:
                late.append(elem)
                continue
            for w in wr.assigner.assign(elem.ts):
                panes.setdefault((k, w), []).append(elem.value)
    result = Fired(fired)
    result.late = late
    return result


class Fired(list):
    """List of (key, Window, result) with a ``.late`` side-output list."""
    late: list = []


# ===========================================================================
# Stage 5 — keyed state + timers (process functions)
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
    """Event-time timers. Timers registered for (key, ts) fire when the
    watermark passes ts."""

    def __init__(self):
        self._timers = set()

    def register(self, key, ts):
        self._timers.add((key, ts))

    def advance(self, watermark):
        """Return (key, ts) timers with ts <= watermark, in (ts, key) order,
        and remove them."""
        ready = sorted((t for t in self._timers if t[1] <= watermark),
                       key=lambda t: (t[1], repr(t[0])))
        for t in ready:
            self._timers.discard(t)
        return ready


class ProcessFunction:
    """Learner subclasses override process_element / on_timer. ``ctx`` gives
    access to the ValueState and TimerService."""

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
    """Drive a KeyedProcessFunction over an event-time stream. Fires timers when
    watermarks advance. Returns the collected output elements in order."""
    _, stream = _drive_chain(kpr.ops)
    state = ValueState()
    timers = TimerService()
    pf = kpr.process_fn
    pf.open(state, timers)
    out_list = []

    def out(x):
        out_list.append(x)

    watermark = None
    for elem in stream:
        if isinstance(elem, Watermark):
            watermark = elem.t if watermark is None else max(watermark, elem.t)
            for (k, ts) in timers.advance(watermark):
                pf._current_key = k
                pf.on_timer(k, ts, out)
        elif isinstance(elem, Event):
            pf._current_key = kpr.key_fn(elem)
            pf.process_element(elem, out)
    return out_list


# ===========================================================================
# Stage 6 — checkpoint barriers (Chandy-Lamport / ABS)
# ===========================================================================
class StatefulSumOperator(Operator):
    """A keyed running-sum operator that can be snapshotted at a barrier. Emits
    (key, running_sum) per event. Used to demonstrate consistent snapshots."""

    def __init__(self, key_fn):
        self.key_fn = key_fn
        self.state = {}
        self.checkpoints = {}  # checkpoint id -> snapshot of state

    def process(self, elem, out):
        if isinstance(elem, Barrier):
            # Snapshot state AT the barrier, then forward the barrier.
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
    operator_states: dict          # op_name -> snapshot
    source_offset: int             # how many source records were consumed


def inject_barriers(records, every: int, start_id: int = 1):
    """Insert a Barrier after every ``every`` data records (deterministic
    schedule — no randomness, no sleeps). Returns a new element list."""
    out = []
    count = 0
    cid = start_id
    for r in records:
        out.append(r)
        if isinstance(r, Event):
            count += 1
            if count % every == 0:
                out.append(Barrier(cid))
                cid += 1
    return out


def run_with_checkpoints(records, key_fn, barrier_every: int):
    """Run a single stateful-sum operator with barriers injected every
    ``barrier_every`` events. Returns (outputs, checkpoints) where checkpoints
    maps checkpoint id -> Checkpoint (operator state + source offset at the
    barrier). This is a one-operator ABS snapshot."""
    marked = inject_barriers(records, barrier_every)
    op = StatefulSumOperator(key_fn)
    outputs = []
    checkpoints = {}
    consumed = 0  # source offset: data records seen so far

    def out(e):
        if isinstance(e, Barrier):
            checkpoints[e.id] = Checkpoint(
                id=e.id,
                operator_states={"sum": op.checkpoints[e.id]},
                source_offset=consumed,
            )
        elif isinstance(e, Event):
            outputs.append((e.key, e.value))

    for elem in marked:
        if isinstance(elem, Event):
            consumed += 1
        op.process(elem, out)
    return outputs, checkpoints


# ===========================================================================
# Stage 7 — recovery from a checkpoint with a replayable source
# ===========================================================================
class ReplayableSource:
    """A source that can replay from an offset (like a Kafka partition). Offset =
    number of records already consumed."""

    def __init__(self, records):
        self.records = list(records)

    def replay_from(self, offset: int):
        return self.records[offset:]


def run_with_recovery(records, key_fn, barrier_every: int, crash_after: int):
    """Run the stateful-sum job, CRASH deterministically after exactly
    ``crash_after`` data records have been processed, then RECOVER from the last
    completed checkpoint and replay the replayable source from that checkpoint's
    offset. Returns the final {key: sum}.

    Exactly-once invariant: the result must equal a clean run with no crash (no
    double-counting of records between the checkpoint and the crash, no loss).
    """
    source = ReplayableSource(records)
    marked = inject_barriers(records, barrier_every)

    op = StatefulSumOperator(key_fn)
    checkpoints = {}
    consumed = 0
    last_completed = None  # (checkpoint_id)

    def out(e):
        nonlocal last_completed
        if isinstance(e, Barrier):
            checkpoints[e.id] = Checkpoint(
                id=e.id,
                operator_states={"sum": dict(op.checkpoints[e.id])},
                source_offset=consumed,
            )
            last_completed = e.id

    crashed = False
    for elem in marked:
        if isinstance(elem, Event):
            if consumed >= crash_after:
                crashed = True
                break
            consumed += 1
        op.process(elem, out)

    if not crashed:
        return dict(op.state)

    # RECOVER: new operator, restore from last completed checkpoint, replay.
    op2 = StatefulSumOperator(key_fn)
    if last_completed is not None:
        cp = checkpoints[last_completed]
        op2.state = dict(cp.operator_states["sum"])
        replay_offset = cp.source_offset
    else:
        replay_offset = 0
    tail = source.replay_from(replay_offset)
    for ev in tail:
        if isinstance(ev, Event):
            op2.process(ev, lambda e: None)
    return dict(op2.state)


# ===========================================================================
# Stage 8 — transactional (2PC) sink  (optional)
# ===========================================================================
class TransactionalSink:
    """A two-phase-commit sink (Flink TwoPhaseCommitSinkFunction).

    - Writes go to a per-checkpoint STAGING transaction (pre-commit).
    - ``pre_commit(cid)`` flushes the open txn as a prepared (pending) txn tied
      to checkpoint ``cid`` — durable but not yet visible.
    - ``commit(cid)`` makes a prepared txn's records VISIBLE (called on
      checkpoint-complete notification).
    - On recovery, prepared-but-not-committed txns are either committed (if their
      checkpoint completed) or ABORTED (discarded). ``committed`` is the visible
      output; it never contains duplicates and never loses a committed record.
    """

    def __init__(self):
        self.open_txn = []            # current in-flight writes
        self.prepared = {}            # cid -> list of records (pre-committed)
        self.committed = []           # visible records (in commit order)
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
    """End-to-end exactly-once with a 2PC sink tied to checkpoint barriers.

    The job maps each source Event to a sink write. On each barrier the sink
    ``pre_commit``s the open transaction (a prepared, durable-but-invisible txn
    keyed by checkpoint id) and records the source offset at that checkpoint. A
    checkpoint's ``commit`` (making its records visible) arrives ``commit_lag``
    barriers later (notification lag).

    The job CRASHES deterministically after exactly ``crash_after`` data records.
    On recovery it: aborts the still-open (not-yet-pre-committed) transaction;
    aborts any prepared txn whose checkpoint had NOT been committed; then replays
    the replayable source from the last COMPLETED checkpoint's offset and
    re-runs, committing the replayed tail.

    Returns the sorted list of visible committed records.

    EOS invariant: the committed output equals the clean no-crash committed
    output — every input appears exactly once (uncommitted staging is aborted and
    the matching records are replayed, never double-committed, never lost).
    """
    marked = inject_barriers(records, barrier_every)
    source = ReplayableSource(records)

    sink = TransactionalSink()
    checkpoint_offsets = {}       # cid -> source offset at pre_commit
    completed = set()             # cids whose commit notification arrived
    pending_commit = []           # cids pre-committed, awaiting notification
    consumed = 0

    def deliver_commits(flush=False):
        # Deliver notifications: a cid commits once `commit_lag` later barriers
        # exist after it (or on drain/flush).
        while pending_commit and (flush or len(pending_commit) > commit_lag):
            due = pending_commit.pop(0)
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
            checkpoint_offsets[elem.id] = consumed
            pending_commit.append(elem.id)
            deliver_commits()

    if not crashed:
        # Job finished: a FINAL checkpoint pre-commits the trailing open txn,
        # then all pending notifications are delivered.
        final_cid = (max(checkpoint_offsets) if checkpoint_offsets else 0) + 1
        sink.pre_commit(final_cid)
        checkpoint_offsets[final_cid] = consumed
        pending_commit.append(final_cid)
        deliver_commits(flush=True)
        return sorted(sink.committed)

    # RECOVER.
    sink.abort_open()
    for cid in list(sink.prepared):
        if cid not in completed:
            sink.abort(cid)        # prepared but never notified -> abort
    last_completed = max(completed) if completed else None
    replay_offset = (checkpoint_offsets[last_completed]
                     if last_completed is not None else 0)

    visible = list(sink.committed)      # survives the crash (already visible)
    sink2 = TransactionalSink()
    rcid = (last_completed or 0) + 1000
    for ev in source.replay_from(replay_offset):
        if isinstance(ev, Event):
            sink2.write((ev.key, ev.value, ev.ts))
    sink2.pre_commit(rcid)
    sink2.commit(rcid)
    visible.extend(sink2.committed)
    return sorted(visible)
