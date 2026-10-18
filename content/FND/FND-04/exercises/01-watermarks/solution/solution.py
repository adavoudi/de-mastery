"""Reference solution for FND-04/01-watermarks."""

from __future__ import annotations

NEG_INF = float("-inf")


class WatermarkGenerator:
    def __init__(self, allowed_lateness: int):
        if allowed_lateness < 0:
            raise ValueError("allowed_lateness must be >= 0")
        self.allowed_lateness = allowed_lateness
        self._max_seen = None
        self._watermark = NEG_INF

    def observe(self, event_time: int) -> int:
        if self._max_seen is None or event_time > self._max_seen:
            self._max_seen = event_time
        candidate = self._max_seen - self.allowed_lateness
        # Never go backwards.
        if candidate > self._watermark:
            self._watermark = candidate
        return self._watermark

    def current(self) -> int:
        return self._watermark


def classify_events(events, allowed_lateness: int, grace=None) -> list[dict]:
    g = allowed_lateness if grace is None else grace
    gen = WatermarkGenerator(allowed_lateness)
    out = []
    for t in events:
        w_before = gen.current()
        if w_before == NEG_INF:
            label = "on_time"
        elif t > w_before:
            label = "on_time"
        elif t >= w_before - g:
            label = "late_accepted"
        else:
            label = "dropped"
        out.append({"event_time": t, "watermark_before": w_before, "label": label})
        gen.observe(t)
    return out
