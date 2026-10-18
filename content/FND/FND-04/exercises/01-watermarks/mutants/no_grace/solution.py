"""Mutant: ignores the grace window entirely, so every event at or below the
watermark is DROPPED and nothing is ever 'late_accepted'. This silently loses late
events that should have been accepted within the grace period."""

from __future__ import annotations

NEG_INF = float("-inf")


class WatermarkGenerator:
    def __init__(self, allowed_lateness: int):
        self.lateness = allowed_lateness
        self._max = None
        self._wm = NEG_INF

    def observe(self, event_time: int) -> int:
        self._max = event_time if self._max is None else max(self._max, event_time)
        self._wm = max(self._wm, self._max - self.lateness)
        return self._wm

    def current(self) -> int:
        return self._wm


def classify_events(events, allowed_lateness: int, grace=None) -> list[dict]:
    gen = WatermarkGenerator(allowed_lateness)
    out = []
    for t in events:
        w = gen.current()
        if w == NEG_INF or t > w:
            label = "on_time"
        else:
            # BUG: no grace window; everything <= watermark is dropped.
            label = "dropped"
        out.append({"event_time": t, "watermark_before": w, "label": label})
        gen.observe(t)
    return out
