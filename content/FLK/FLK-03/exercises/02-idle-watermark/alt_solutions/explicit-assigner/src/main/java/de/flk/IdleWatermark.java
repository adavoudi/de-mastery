package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * Alt A: an explicit SerializableTimestampAssigner, and withIdleness applied
 * BEFORE withTimestampAssigner (the decorators compose in any order).
 */
public class IdleWatermark {

    public static WatermarkStrategy<Event> strategy(long outOfOrdernessMs, long idleMs) {
        SerializableTimestampAssigner<Event> assigner =
                (event, recordTimestamp) -> event.tsMs;
        return WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs))
                .withIdleness(Duration.ofMillis(idleMs))
                .withTimestampAssigner(assigner);
    }
}
