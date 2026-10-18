"""Alt solution 1 for FND-04.

Different insight: since max_seen only ever grows, (max_seen - allowed_lateness) is
ALREADY monotonic non-decreasing, so no separate clamp against a stored watermark is
needed. The watermark is derived on demand from max_seen.
"""

from __future__ import annotations

NEG_INF = float("-inf")


class WatermarkGenerator:
    def __init__(self, allowed_lateness: int):
        if allowed_lateness < 0:
            raise ValueError("allowed_lateness must be >= 0")
        self.lateness = allowed_lateness
        self._max_seen = None

    def observe(self, event_time: int) -> int:
        self._max_seen = event_time if self._max_seen is None \
            else max(self._max_seen, event_time)
        return self.current()

    def current(self) -> int:
        if self._max_seen is None:
            return NEG_INF
        return self._max_seen - self.lateness


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
