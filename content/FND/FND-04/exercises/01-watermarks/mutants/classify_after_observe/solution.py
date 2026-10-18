"""Mutant: observes the event FIRST, then classifies against the post-event
watermark. An event is then compared to a watermark it just advanced, so events
near the frontier get mislabeled (e.g. the first event can be 'late_accepted')."""

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
    g = allowed_lateness if grace is None else grace
    gen = WatermarkGenerator(allowed_lateness)
    out = []
    for t in events:
        w = gen.observe(t)  # BUG: observe before reading the watermark
        if w == NEG_INF or t > w:
            label = "on_time"
        elif t >= w - g:
            label = "late_accepted"
        else:
            label = "dropped"
        out.append({"event_time": t, "watermark_before": w, "label": label})
    return out
