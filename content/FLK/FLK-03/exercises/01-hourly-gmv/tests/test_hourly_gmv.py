"""FLK-03/01 — hourly (tumbling event-time) GMV with a late side output.

A deterministic ``EventSource`` (shipped as extra Java sources) emits events with
EXPLICIT per-record watermarks, so lateness is fully controlled — no
WatermarkStrategy timing, no sleep, no wall-clock. The learner implements
``de.flk.HourlyGmv.build(stream, lateTag, windowMs)`` to window per company with
a late side output. The driver collects the main window results and the late
side-output stream and prints ``DE_RESULT={"windows":[...],"late":[...]}``.

Oracle (G6): an independent Python computation of the per-(company,window) sums
over the ON-TIME events, and the set of LATE events (those whose timestamp falls
in a window the watermark already closed before the event arrived).

Named mutants per the FLK syllabus: a late-event mutant (no side output / counts
late events into the window) and ... see the companion exercise 02-idleness for
the idleness mutant.

SKIPs cleanly when the Flink dist / JDK are absent.
"""

import json
from pathlib import Path

WINDOW_MS = 1000

# (companyId, amountCents, tsMs, watermarkAfter)
# c1: 10@100, 20@200 in window [0,1000); wm->2000 closes it; then 99@50 arrives LATE.
# c2: 30@1500 in window [1000,2000); wm->2000.
EVENTS = [
    ("c1", 10, 100, 150),
    ("c1", 20, 200, 250),
    ("c2", 30, 1500, 2000),   # wm jumps to 2000 -> closes c1's [0,1000)
    ("c1", 99, 50, 5000),     # LATE: belongs to [0,1000) but arrives after close
]

HARNESS = Path(__file__).parent / "java"


def _oracle(events, window_ms):
    """Returns (windows, late) where windows is {company|winStart: sum} over
    on-time events and late is the set of 'company|ts|amt' late events."""
    # a watermark that has passed window_end closes the window. Replay in order:
    windows = {}
    late = []
    current_wm = -(1 << 62)
    for company, amt, ts, wm_after in events:
        win_start = (ts // window_ms) * window_ms
        win_end = win_start + window_ms
        if win_end <= current_wm:
            # the window already closed before this event -> late
            late.append(f"{company}|{ts}|{amt}")
        else:
            key = f"{company}|{win_start}"
            windows[key] = windows.get(key, 0) + amt
        current_wm = max(current_wm, wm_after)
    win_rows = [f"{k}|{v}" for k, v in windows.items()]
    return win_rows, late


_DRIVER = r"""
import de.flk.HourlyGmv;
import de.flk.harness.Event;
import de.flk.harness.EventSource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class DeDriver {
  public static void main(String[] a) throws Exception {
    long windowMs = %d;
    List<Event> events = new ArrayList<>();
    List<Long> wms = new ArrayList<>();
%s
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    OutputTag<Event> lateTag =
        new OutputTag<Event>("late", TypeInformation.of(Event.class)) {};

    DataStream<Event> src = env.addSource(new EventSource(events, wms),
        TypeInformation.of(Event.class));
    SingleOutputStreamOperator<String> main = HourlyGmv.build(src, lateTag, windowMs);

    List<String> windowRows = new ArrayList<>();
    try (CloseableIterator<String> it = main.executeAndCollect()) {
      while (it.hasNext()) windowRows.add(it.next());
    }
    List<String> lateRows = new ArrayList<>();
    try (CloseableIterator<Event> it = main.getSideOutput(lateTag).executeAndCollect()) {
      while (it.hasNext()) { Event e = it.next(); lateRows.add(e.companyId + "|" + e.tsMs + "|" + e.amountCents); }
    }
    System.out.println("DE_RESULT={\"windows\":" + arr(windowRows) + ",\"late\":" + arr(lateRows) + "}");
  }

  static String arr(List<String> xs) {
    StringBuilder b = new StringBuilder("[");
    for (int i = 0; i < xs.size(); i++) { if (i > 0) b.append(","); b.append("\"").append(xs.get(i)).append("\""); }
    return b.append("]").toString();
  }
}
"""


def _driver():
    adds = []
    for company, amt, ts, wm in EVENTS:
        adds.append(f'    events.add(new Event("{company}", {amt}L, {ts}L)); wms.add({wm}L);')
    return _DRIVER % (WINDOW_MS, "\n".join(adds))


def _run(flink_java, submission_dir):
    extra = [HARNESS / "de" / "flk" / "harness" / "Event.java",
             HARNESS / "de" / "flk" / "harness" / "EventSource.java"]
    r = flink_java(submission_dir, _driver(), extra_sources=extra)
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2500:]}"
    assert isinstance(r.result, dict), f"bad result {r.result!r}"
    return r.result


def test_window_sums_match_oracle(flink_java, submission_dir):
    out = _run(flink_java, submission_dir)
    exp_windows, _ = _oracle(EVENTS, WINDOW_MS)
    assert set(out["windows"]) == set(exp_windows), (
        f"\n got  {sorted(out['windows'])}\n want {sorted(exp_windows)}")


def test_late_event_goes_to_side_output(flink_java, submission_dir):
    # the 99@50 event is late (its window closed) -> must appear in the late
    # side output and must NOT be added into the window sum.
    out = _run(flink_java, submission_dir)
    _, exp_late = _oracle(EVENTS, WINDOW_MS)
    assert set(out["late"]) == set(exp_late), (
        f"\n got  {sorted(out['late'])}\n want {sorted(exp_late)}")
    # c1's [0,1000) window must be 30 (10+20), NOT 129 (would mean the late event
    # was wrongly folded in).
    assert "c1|0|30" in out["windows"], out["windows"]
    assert "c1|0|129" not in out["windows"], out["windows"]


def test_no_window_uses_processing_time(flink_java, submission_dir):
    # event-time windowing must place c2's 1500ms event in [1000,2000), not in a
    # processing-time bucket. The window start must be a multiple of windowMs.
    out = _run(flink_java, submission_dir)
    assert "c2|1000|30" in out["windows"], out["windows"]
    for row in out["windows"]:
        start = int(row.split("|")[1])
        assert start % WINDOW_MS == 0, row
