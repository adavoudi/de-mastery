package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * FLK-03 starter — NOT implemented yet (the tests must fail: G1).
 *
 * Return a WatermarkStrategy for the event stream that tolerates bounded
 * out-of-orderness, assigns event time from Event.tsMs, and marks a quiet
 * partition idle after idleMs so it does not freeze the watermark minimum.
 */
public class IdleWatermark {

    public static WatermarkStrategy<Event> strategy(long outOfOrdernessMs, long idleMs) {
        // TODO: forBoundedOutOfOrderness(..).withTimestampAssigner(..).withIdleness(..)
        return WatermarkStrategy.noWatermarks();
    }
}
