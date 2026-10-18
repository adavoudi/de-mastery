package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * MUTANT: no timestamp assigner — the generator never sees the event timestamp,
 * so the emitted watermark is not (maxEventTs - bound).
 */
public class IdleWatermark {

    public static WatermarkStrategy<Event> strategy(long outOfOrdernessMs, long idleMs) {
        return WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs))
                // WRONG: no .withTimestampAssigner(...)
                .withIdleness(Duration.ofMillis(idleMs));
    }
}
