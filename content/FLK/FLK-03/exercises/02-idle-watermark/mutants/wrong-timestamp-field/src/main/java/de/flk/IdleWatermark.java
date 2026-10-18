package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/** MUTANT: timestamp assigner reads the WRONG field (amountCents) instead of tsMs. */
public class IdleWatermark {

    public static WatermarkStrategy<Event> strategy(long outOfOrdernessMs, long idleMs) {
        return WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs))
                .withTimestampAssigner((e, ts) -> e.amountCents)   // WRONG field
                .withIdleness(Duration.ofMillis(idleMs));
    }
}
