package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * Alt B: build the base strategy via forGenerator(..) with an explicit
 * BoundedOutOfOrdernessWatermarks generator, then attach the timestamp assigner
 * and idleness. Same observable behaviour via the lower-level API.
 */
public class IdleWatermark {

    public static WatermarkStrategy<Event> strategy(long outOfOrdernessMs, long idleMs) {
        return WatermarkStrategy
                .<Event>forGenerator(ctx ->
                        new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(outOfOrdernessMs)))
                .withTimestampAssigner((e, ts) -> e.tsMs)
                .withIdleness(Duration.ofMillis(idleMs));
    }
}
