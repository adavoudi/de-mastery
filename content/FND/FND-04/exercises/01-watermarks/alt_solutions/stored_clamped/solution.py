"""Alt solution 2 for FND-04.

Different structure: keeps an explicit stored watermark that is clamped on every
observe (defensive style), and classifies using a small helper that returns the label
from the two threshold comparisons.
"""

from __future__ import annotations

NEG_INF = float("-inf")


def _label(t, w, g) -> str:
    if w == NEG_INF:
        return "on_time"
    if t > w:
        return "on_time"
    if t >= w - g:
        return "late_accepted"
    return "dropped"


class WatermarkGenerator:
    def __init__(self, allowed_lateness: int):
        if allowed_lateness < 0:
            raise ValueError("allowed_lateness must be >= 0")
        self.lateness = allowed_lateness
        self._seen = []          # all event times observed
        self._wm = NEG_INF

    def observe(self, event_time: int) -> int:
        self._seen.append(event_time)
        self._wm = max(self._wm, max(self._seen) - self.lateness)
        return self._wm

    def current(self) -> int:
        return self._wm


def classify_events(events, allowed_lateness: int, grace=None) -> list[dict]:
    g = allowed_lateness if grace is None else grace
    gen = WatermarkGenerator(allowed_lateness)
    rows = []
    for t in events:
        w = gen.current()
        rows.append({"event_time": t, "watermark_before": w,
                     "label": _label(t, w, g)})
        gen.observe(t)
    return rows
