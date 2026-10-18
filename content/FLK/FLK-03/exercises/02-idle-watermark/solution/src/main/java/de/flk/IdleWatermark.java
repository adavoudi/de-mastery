package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * FLK-03 reference: a watermark strategy for an event stream that
 *   1) tolerates bounded out-of-orderness,
 *   2) assigns the event timestamp from {@code Event.tsMs}, and
 *   3) marks a partition IDLE after {@code idleMs} of no events, so a quiet
 *      partition does not freeze the combined watermark minimum.
 */
public class IdleWatermark {

    public static WatermarkStrategy<Event> strategy(long outOfOrdernessMs, long idleMs) {
        return WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs))
                .withTimestampAssigner((e, ts) -> e.tsMs)
                .withIdleness(Duration.ofMillis(idleMs));
    }
}
