package de.flk;

import de.flk.harness.Event;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/** MUTANT (idleness): omits withIdleness — a quiet partition never goes idle and freezes the watermark. */
public class IdleWatermark {

    public static WatermarkStrategy<Event> strategy(long outOfOrdernessMs, long idleMs) {
        return WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs))
                .withTimestampAssigner((e, ts) -> e.tsMs);
        // WRONG: no .withIdleness(...) -> idle partitions pin the watermark minimum.
    }
}
