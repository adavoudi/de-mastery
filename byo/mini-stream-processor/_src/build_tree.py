#!/usr/bin/env python3
"""Build the FLK-BYO stage tree from the cumulative sources in _src/.

For every stage dir:
  solution/processor.py        <- processor_full.py (whole cumulative reference)
  alt_solutions/pull-based/processor.py <- processor_alt.py (genuinely different)
  mutants/<name>/processor.py  <- processor_full.py with ONE string-patched defect
  mutants/<name>/WHY.md        <- one-line reason

Mutants are stage-SPECIFIC: each must still pass stages 1..N-1 (so the patched
code path belongs to stage N) but break stage N. This script is in _src/ which
the BYO runner ignores (it only scans starter/ and stage-*/).
"""

from __future__ import annotations

import pathlib

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
FULL = (HERE / "processor_full.py").read_text()
ALT = (HERE / "processor_alt.py").read_text()

STAGES = [
    "stage-01-operator-graph",
    "stage-02-keyed-partitioning",
    "stage-03-event-time-watermarks",
    "stage-04-windows-triggers",
    "stage-05-keyed-state-timers",
    "stage-06-checkpoint-barriers",
    "stage-07-recovery-replay",
    "stage-08-transactional-sink",
]


def patch(src: str, old: str, new: str) -> str:
    if old not in src:
        raise SystemExit(f"PATCH TARGET NOT FOUND:\n{old!r}")
    if src.count(old) != 1:
        raise SystemExit(f"PATCH TARGET NOT UNIQUE ({src.count(old)}): {old!r}")
    return src.replace(old, new)


# Each entry: stage_index(0-based) -> {mutant_name: (why, [(old,new), ...])}
MUTANTS: dict[int, dict] = {
    # ---------------- Stage 1 ----------------
    0: {
        "sink-keeps-markers": (
            "Sink collects Watermark/Barrier markers instead of dropping them.",
            [("""    def process(self, elem, out):
        if not isinstance(elem, (Watermark, Barrier)):
            self.collected.append(elem)""",
              """    def process(self, elem, out):
        self.collected.append(elem)""")],
        ),
        "source-reversed": (
            "Source emits records in reverse, breaking push-order preservation.",
            [("""    def run(self, out):
        for r in self.records:
            out(r)""",
              """    def run(self, out):
        for r in reversed(self.records):
            out(r)""")],
        ),
        "filter-inverted": (
            "FilterOp keeps the records the predicate REJECTS (inverted filter).",
            [("""        elif self.fn(elem):
            out(elem)""",
              """        elif not self.fn(elem):
            out(elem)""")],
        ),
    },
    # ---------------- Stage 2 ----------------
    1: {
        "pyhash-partition": (
            "partition_of uses Python's salted hash() -> not stable across procs.",
            [("""    \"\"\"Which partition (worker) a key is routed to. Flink: murmurHash % n.\"\"\"
    return stable_hash(key) % num_partitions""",
              """    \"\"\"Which partition (worker) a key is routed to. Flink: murmurHash % n.\"\"\"
    return hash(key) % num_partitions""")],
        ),
        "partition-no-mod": (
            "partition_of forgets the modulo -> worker index out of range.",
            [("""    \"\"\"Which partition (worker) a key is routed to. Flink: murmurHash % n.\"\"\"
    return stable_hash(key) % num_partitions""",
              """    \"\"\"Which partition (worker) a key is routed to. Flink: murmurHash % n.\"\"\"
    return stable_hash(key)""")],
        ),
        "reduce-overwrites": (
            "Keyed reduce overwrites with the last value instead of reducing.",
            [("""        if k in state:
            state[k] = keyed.reduce_fn(state[k], ev.value)
        else:
            state[k] = ev.value""",
              """        state[k] = ev.value""")],
        ),
    },
    # ---------------- Stage 3 ----------------
    2: {
        "assign-emits-nonadvancing": (
            "assign_watermarks emits a watermark after EVERY event, even when it "
            "did not advance -> non-monotone (duplicate) watermarks.",
            [("""        wm = gen.current_watermark()
        if wm is not None and (last is None or wm > last):
            out.append(Watermark(wm))
            last = wm""",
              """        wm = gen.current_watermark()
        if wm is not None:
            out.append(Watermark(wm))
            last = wm""")],
        ),
        "merge-uses-max": (
            "merge_watermarks combines inputs with max, not min across inputs.",
            [("""                if len(have) == len(streams):
                    combined = watermark_min(have)""",
              """                if len(have) == len(streams):
                    combined = max(have)""")],
        ),
        "watermark-ignores-delay": (
            "current_watermark ignores the out-of-orderness delay.",
            [("""        wm = self.max_ts - self.delay
        if self.last_wm is None or wm > self.last_wm:""",
              """        wm = self.max_ts
        if self.last_wm is None or wm > self.last_wm:""")],
        ),
    },
    # ---------------- Stage 4 ----------------
    3: {
        "tumbling-unaligned": (
            "TumblingWindows.assign starts the window at ts, not the aligned start.",
            [("""    def assign(self, ts: int) -> list[Window]:
        start = (ts // self.size) * self.size
        return [Window(start, start + self.size)]""",
              """    def assign(self, ts: int) -> list[Window]:
        start = ts
        return [Window(start, start + self.size)]""")],
        ),
        "window-fires-on-start": (
            "Window fires when the watermark passes its START, not its END (early).",
            [("""        ready = [kw for kw in panes
                 if watermark is not None and kw[1].end <= watermark]""",
              """        ready = [kw for kw in panes
                 if watermark is not None and kw[1].start <= watermark]""")],
        ),
        "no-late-drop": (
            "Late events past the watermark are not dropped to the side output.",
            [("""            if watermark is not None and \\
                    max_end + wr.allowed_lateness <= watermark:
                late.append(elem)
                continue""",
              """            if False:
                late.append(elem)
                continue""")],
        ),
    },
    # ---------------- Stage 5 ----------------
    4: {
        "timers-ignore-watermark": (
            "TimerService.advance fires ALL timers, ignoring the watermark.",
            [("""        ready = sorted((t for t in self._timers if t[1] <= watermark),
                       key=lambda t: (t[1], repr(t[0])))""",
              """        ready = sorted((t for t in self._timers),
                       key=lambda t: (t[1], repr(t[0])))""")],
        ),
        "timers-not-removed": (
            "Fired timers are not removed, so they fire again on every watermark.",
            [("""        for t in ready:
            self._timers.discard(t)
        return ready""",
              """        return ready""")],
        ),
        "process-wrong-key": (
            "process_element runs under a constant key, so per-key state collides.",
            [("""        elif isinstance(elem, Event):
            pf._current_key = kpr.key_fn(elem)
            pf.process_element(elem, out)""",
              """        elif isinstance(elem, Event):
            pf._current_key = "__all__"
            pf.process_element(elem, out)""")],
        ),
    },
    # ---------------- Stage 6 ----------------
    5: {
        "snapshot-no-copy": (
            "Barrier snapshot stores a live reference, so later writes corrupt it.",
            [("""        if isinstance(elem, Barrier):
            # Snapshot state AT the barrier, then forward the barrier.
            self.checkpoints[elem.id] = dict(self.state)
            out(elem)""",
              """        if isinstance(elem, Barrier):
            # Snapshot state AT the barrier, then forward the barrier.
            self.checkpoints[elem.id] = self.state
            out(elem)""")],
        ),
        "barrier-not-forwarded": (
            "Barrier is snapshotted but not forwarded downstream.",
            [("""        if isinstance(elem, Barrier):
            # Snapshot state AT the barrier, then forward the barrier.
            self.checkpoints[elem.id] = dict(self.state)
            out(elem)""",
              """        if isinstance(elem, Barrier):
            # Snapshot state AT the barrier, then forward the barrier.
            self.checkpoints[elem.id] = dict(self.state)""")],
        ),
        "offset-off-by-one": (
            "source_offset is recorded one record too high (inconsistent cut).",
            [("""            checkpoints[e.id] = Checkpoint(
                id=e.id,
                operator_states={"sum": op.checkpoints[e.id]},
                source_offset=consumed,
            )
        elif isinstance(e, Event):
            outputs.append((e.key, e.value))""",
              """            checkpoints[e.id] = Checkpoint(
                id=e.id,
                operator_states={"sum": op.checkpoints[e.id]},
                source_offset=consumed + 1,
            )
        elif isinstance(e, Event):
            outputs.append((e.key, e.value))""")],
        ),
    },
    # ---------------- Stage 7 ----------------
    6: {
        "replay-from-zero": (
            "Recovery replays the source from offset 0 onto restored state (double count).",
            [("""        op2.state = dict(cp.operator_states["sum"])
        replay_offset = cp.source_offset""",
              """        op2.state = dict(cp.operator_states["sum"])
        replay_offset = 0""")],
        ),
        "resume-from-crash": (
            "Recovery replays from the crash point, losing records since the checkpoint.",
            [("""        op2.state = dict(cp.operator_states["sum"])
        replay_offset = cp.source_offset""",
              """        op2.state = dict(cp.operator_states["sum"])
        replay_offset = crash_after""")],
        ),
        "no-state-restore": (
            "Recovery seeks to the checkpoint offset but starts from empty state (loss).",
            [("""        op2.state = dict(cp.operator_states["sum"])
        replay_offset = cp.source_offset""",
              """        op2.state = {}
        replay_offset = cp.source_offset""")],
        ),
    },
    # ---------------- Stage 8 ----------------
    7: {
        "commit-on-precommit": (
            "pre_commit immediately commits (no 2PC) -> uncommitted output survives a crash.",
            [("""    def pre_commit(self, cid: int):
        self.prepared[cid] = list(self.open_txn)
        self.open_txn = []""",
              """    def pre_commit(self, cid: int):
        self.prepared[cid] = list(self.open_txn)
        self.committed.extend(self.open_txn)
        self._committed_cids.add(cid)
        self.open_txn = []""")],
        ),
        "no-abort-on-recovery": (
            "Recovery does not abort prepared-but-unnotified txns -> duplicates on replay.",
            [("""    sink.abort_open()
    for cid in list(sink.prepared):
        if cid not in completed:
            sink.abort(cid)        # prepared but never notified -> abort""",
              """    sink.abort_open()
    for cid in list(sink.prepared):
        if cid not in completed:
            sink.commit(cid)       # BUG: commit instead of abort""")],
        ),
        "sink-replay-from-zero": (
            "Recovery replays the sink source from offset 0 -> committed records duplicated.",
            [("""    replay_offset = (checkpoint_offsets[last_completed]
                     if last_completed is not None else 0)""",
              """    replay_offset = 0""")],
        ),
    },
}


def write(path: pathlib.Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def main():
    import shutil
    for i, stage in enumerate(STAGES):
        sdir = ROOT / stage
        if not sdir.is_dir():
            raise SystemExit(f"missing stage dir: {sdir}")
        # clean generated dirs (keep README.md, stage.yaml, tests/)
        for sub in ("solution", "alt_solutions", "mutants"):
            if (sdir / sub).exists():
                shutil.rmtree(sdir / sub)
        # solution (cumulative full reference)
        write(sdir / "solution" / "processor.py", FULL)
        # alt (genuinely different, cumulative)
        write(sdir / "alt_solutions" / "pull-based" / "processor.py", ALT)
        # mutants
        for name, (why, patches) in MUTANTS[i].items():
            src = FULL
            for old, new in patches:
                src = patch(src, old, new)
            write(sdir / "mutants" / name / "processor.py", src)
            write(sdir / "mutants" / name / "WHY.md", why + "\n")
        print(f"{stage}: solution + alt + {len(MUTANTS[i])} mutants")


if __name__ == "__main__":
    main()
