"""FLK-03/02 — a watermark strategy that survives an idle partition.

The learner implements ``de.flk.IdleWatermark.strategy(outOfOrdernessMs, idleMs)``
returning a WatermarkStrategy<Event> with bounded out-of-orderness + a timestamp
assigner (event.tsMs) + idleness.

We grade it DETERMINISTICALLY by driving the strategy's own TimestampAssigner and
WatermarkGenerator with a FAKE, manually-advanced clock (no sleep, no wall-clock)
— exactly the API Flink uses internally:

  * call createTimestampAssigner(..).extractTimestamp(event, recordTs) with a
    recordTs DIFFERENT from event.tsMs -> it must return event.tsMs, proving the
    strategy wired withTimestampAssigner((e,ts)->e.tsMs) and not the passthrough;
  * then advance the fake clock past idleMs with no events and pump periodic
    emits -> the generator must call markIdle(), proving withIdleness is set.

A strategy WITHOUT idleness never calls markIdle -> the idleness mutant fails.

SKIPs cleanly when the Flink dist / JDK are absent.
"""

from pathlib import Path

HARNESS = Path(__file__).parent / "java"

OUT_OF_ORDERNESS_MS = 5
IDLE_MS = 500

_DRIVER = r"""
import de.flk.IdleWatermark;
import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.clock.RelativeClock;

public class DeDriver {
  static long clockMs = 1000;   // a manually-advanced fake clock

  public static void main(String[] a) throws Exception {
    long ooo = %d, idle = %d;
    WatermarkStrategy<Event> ws = IdleWatermark.strategy(ooo, idle);

    // (1) timestamp assigner: pass a record timestamp DIFFERENT from event.tsMs
    // and require the assigner to return event.tsMs.
    TimestampAssignerSupplier.Context taCtx = () -> UnregisteredMetricsGroup.createOperatorMetricGroup();
    TimestampAssigner<Event> ta = ws.createTimestampAssigner(taCtx);
    long assigned = ta.extractTimestamp(new Event("c1", 10, 777L), -1L);

    // (2) idleness: drive the generator with a controllable clock.
    RelativeClock clock = new RelativeClock() {
      public long relativeTimeMillis() { return clockMs; }
      public long relativeTimeNanos() { return clockMs * 1_000_000L; }
    };
    WatermarkGeneratorSupplier.Context ctx = new WatermarkGeneratorSupplier.Context() {
      public MetricGroup getMetricGroup() { return UnregisteredMetricsGroup.createOperatorMetricGroup(); }
      public RelativeClock getInputActivityClock() { return clock; }
    };
    WatermarkGenerator<Event> gen = ws.createWatermarkGenerator(ctx);
    final boolean[] idleFlag = { false };
    WatermarkOutput out = new WatermarkOutput() {
      public void emitWatermark(Watermark w) {}
      public void markIdle() { idleFlag[0] = true; }
      public void markActive() { idleFlag[0] = false; }
    };
    // feed two events (use the ASSIGNED timestamp, as a real pipeline would)
    gen.onEvent(new Event("c1", 10, 100L), ta.extractTimestamp(new Event("c1", 10, 100L), -1L), out);
    gen.onEvent(new Event("c1", 20, 200L), ta.extractTimestamp(new Event("c1", 20, 200L), -1L), out);
    gen.onPeriodicEmit(out);               // activity seen
    gen.onPeriodicEmit(out);               // records start of inactivity at clockMs
    clockMs += idle * 1000;                // advance well past the idle timeout
    gen.onPeriodicEmit(out);               // should mark idle

    System.out.println("DE_RESULT={\"assigned_ts\":" + assigned
        + ",\"went_idle\":" + idleFlag[0] + "}");
  }
}
"""


def _driver():
    return _DRIVER % (OUT_OF_ORDERNESS_MS, IDLE_MS)


def _run(flink_java, submission_dir):
    extra = [HARNESS / "de" / "flk" / "harness" / "Event.java"]
    r = flink_java(submission_dir, _driver(), extra_sources=extra)
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2500:]}"
    assert isinstance(r.result, dict), f"bad result {r.result!r}"
    return r.result


def test_timestamp_assigner_uses_event_time(flink_java, submission_dir):
    # the assigner must return event.tsMs (777) even though the record timestamp
    # passed in is -1. A strategy with no timestamp assigner would return the
    # passthrough record timestamp (-1), not 777.
    out = _run(flink_java, submission_dir)
    assert out["assigned_ts"] == 777, out


def test_partition_goes_idle(flink_java, submission_dir):
    # with withIdleness configured, a quiet partition is marked idle; without it,
    # markIdle is never called.
    out = _run(flink_java, submission_dir)
    assert out["went_idle"] is True, out
