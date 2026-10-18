"""mini-stream-processor — your cumulative codebase (starter).

You grow THIS single file across all 8 stages. The dataclasses, operator classes
and the function signatures below are the stable public API the tests call; do
not rename them. Replace each ``raise NotImplementedError`` as you reach its
stage. Pure Python stdlib only — no Flink, no JVM.

Public surface (see each stage README for the full contract):

  Stage 1  Environment().source(recs).map(fn).filter(fn).sink(); collect(stream)
  Stage 2  key_by(fn).reduce(fn, parallelism); run_parallel; partition_of
  Stage 3  assign_watermarks(events, delay); watermark_min; merge_watermarks
  Stage 4  TumblingWindows / SlidingWindows; .window(assigner).aggregate(fn); run_windows
  Stage 5  ValueState, TimerService, ProcessFunction; .process(fn); run_process
  Stage 6  Barrier, inject_barriers, run_with_checkpoints
  Stage 7  ReplayableSource, run_with_recovery
  Stage 8  TransactionalSink, run_exactly_once_sink  (optional)
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
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
# Stage 1 — operators + push-based execution
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
        raise NotImplementedError("stage 1: push each record downstream")


class MapOp(Operator):
    def __init__(self, fn):
        self.fn = fn

    def process(self, elem, out):
        raise NotImplementedError("stage 1: apply fn to data; forward markers")


class FilterOp(Operator):
    def __init__(self, fn):
        self.fn = fn

    def process(self, elem, out):
        raise NotImplementedError("stage 1: keep passing data; forward markers")


class SinkOp(Operator):
    def __init__(self):
        self.collected = []

    def process(self, elem, out):
        raise NotImplementedError("stage 1: collect data events; drop markers")


class Stream:
    def __init__(self, env, ops):
        self.env = env
        self.ops = ops

    def map(self, fn) -> "Stream":
        return Stream(self.env, self.ops + [MapOp(fn)])

    def filter(self, fn) -> "Stream":
        return Stream(self.env, self.ops + [FilterOp(fn)])

    def key_by(self, key_fn) -> "KeyedStream":
        return KeyedStream(self.env, self.ops, key_fn)

    def sink(self) -> "Stream":
        return Stream(self.env, self.ops + [SinkOp()])


class Environment:
    def source(self, records) -> Stream:
        return Stream(self, [Source(records)])


def _drive_chain(ops):
    """Push every source record through the operator chain. Returns the terminal
    SinkOp (if any) and the full terminal output list."""
    raise NotImplementedError("stage 1: build the push pipeline and run it")


def collect(stream: Stream) -> list:
    raise NotImplementedError("stage 1: run the stream, return sink events")


# ===========================================================================
# Stage 2 — keyed partitioning across workers
# ===========================================================================
def stable_hash(value) -> int:
    return int.from_bytes(
        hashlib.blake2b(repr(value).encode(), digest_size=8).digest(), "big")


def partition_of(key, num_partitions: int) -> int:
    raise NotImplementedError("stage 2: route a key to a worker partition")


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


def run_parallel(keyed: "KeyedResult") -> dict:
    raise NotImplementedError("stage 2: keyed reduce across workers")


def worker_assignment(keys, num_partitions: int) -> dict:
    raise NotImplementedError("stage 2: {key: worker_index}")


# ===========================================================================
# Stage 3 — event time + watermarks
# ===========================================================================
def watermark_min(input_watermarks) -> int:
    raise NotImplementedError("stage 3: output watermark = min across inputs")


class BoundedOutOfOrderness:
    def __init__(self, delay: int):
        self.delay = delay
        self.max_ts = None
        self.last_wm = None

    def on_event(self, ts: int):
        raise NotImplementedError("stage 3: track max ts seen")

    def current_watermark(self):
        raise NotImplementedError("stage 3: max_ts - delay, monotone")


def assign_watermarks(events, delay: int):
    raise NotImplementedError("stage 3: interleave advancing watermarks")


def merge_watermarks(streams):
    raise NotImplementedError("stage 3: merge inputs, wm = min across inputs")


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

    def assign(self, ts: int) -> list:
        raise NotImplementedError("stage 4: one window per ts")


class SlidingWindows:
    def __init__(self, size: int, slide: int):
        self.size = size
        self.slide = slide

    def assign(self, ts: int) -> list:
        raise NotImplementedError("stage 4: all windows containing ts")


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


class Fired(list):
    late: list = []


def run_windows(wr: "WindowResult"):
    raise NotImplementedError("stage 4: watermark-triggered windowed aggregate")


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
        raise NotImplementedError("stage 5: fire timers with ts <= watermark")


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


def run_process(kpr: "KeyedProcessResult") -> list:
    raise NotImplementedError("stage 5: drive process fn, fire timers on wm")


# ===========================================================================
# Stage 6 — checkpoint barriers
# ===========================================================================
class StatefulSumOperator(Operator):
    def __init__(self, key_fn):
        self.key_fn = key_fn
        self.state = {}
        self.checkpoints = {}

    def process(self, elem, out):
        raise NotImplementedError("stage 6: running sum; snapshot on Barrier")


@dataclass
class Checkpoint:
    id: int
    operator_states: dict
    source_offset: int


def inject_barriers(records, every: int, start_id: int = 1):
    raise NotImplementedError("stage 6: insert a Barrier every `every` events")


def run_with_checkpoints(records, key_fn, barrier_every: int):
    raise NotImplementedError("stage 6: run with ABS snapshots")


# ===========================================================================
# Stage 7 — recovery from a checkpoint with a replayable source
# ===========================================================================
class ReplayableSource:
    def __init__(self, records):
        self.records = list(records)

    def replay_from(self, offset: int):
        raise NotImplementedError("stage 7: return records[offset:]")


def run_with_recovery(records, key_fn, barrier_every: int, crash_after: int):
    raise NotImplementedError("stage 7: crash, restore last checkpoint, replay")


# ===========================================================================
# Stage 8 — transactional (2PC) sink (optional)
# ===========================================================================
class TransactionalSink:
    def __init__(self):
        self.open_txn = []
        self.prepared = {}
        self.committed = []
        self._committed_cids = set()

    def write(self, record):
        raise NotImplementedError("stage 8: buffer into the open transaction")

    def pre_commit(self, cid: int):
        raise NotImplementedError("stage 8: flush open txn as prepared(cid)")

    def commit(self, cid: int):
        raise NotImplementedError("stage 8: make prepared(cid) visible, once")

    def abort(self, cid: int):
        raise NotImplementedError("stage 8: discard prepared(cid)")

    def abort_open(self):
        raise NotImplementedError("stage 8: discard the open txn")


def run_exactly_once_sink(records, barrier_every: int, crash_after: int,
                          commit_lag: int = 0):
    raise NotImplementedError("stage 8: 2PC sink, crash + recover, EOS output")
