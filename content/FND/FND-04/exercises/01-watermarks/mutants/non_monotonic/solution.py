"""Mutant: watermark tracks the LAST event time instead of the max, so a late
small-timestamp event drags the watermark backwards. Violates monotonicity."""

from __future__ import annotations

NEG_INF = float("-inf")


class WatermarkGenerator:
    def __init__(self, allowed_lateness: int):
        self.lateness = allowed_lateness
        self._wm = NEG_INF
        self._any = False

    def observe(self, event_time: int) -> int:
        self._any = True
        # BUG: uses the current event, not the running max -> can decrease.
        self._wm = event_time - self.lateness
        return self._wm

    def current(self) -> int:
        return self._wm if self._any else NEG_INF


def classify_events(events, allowed_lateness: int, grace=None) -> list[dict]:
    g = allowed_lateness if grace is None else grace
    gen = WatermarkGenerator(allowed_lateness)
    out = []
    for t in events:
        w = gen.current()
        if w == NEG_INF or t > w:
            label = "on_time"
        elif t >= w - g:
            label = "late_accepted"
        else:
            label = "dropped"
        out.append({"event_time": t, "watermark_before": w, "label": label})
        gen.observe(t)
    return out
