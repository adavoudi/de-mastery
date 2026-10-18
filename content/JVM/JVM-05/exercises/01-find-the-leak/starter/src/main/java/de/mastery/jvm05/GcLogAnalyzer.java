package de.mastery.jvm05;

import java.util.List;

/** Parse a unified GC log and decide whether the JVM was leaking. Replace the TODOs. */
public class GcLogAnalyzer {

    public record GcEvent(int id, long beforeMb, long afterMb, long totalMb, boolean full) {}

    public enum Diagnosis { OLD_GEN_LEAK, HEALTHY_CHURN, UNKNOWN }

    /** Parse GC pause lines into events, in order. Skip non-pause lines. */
    public static List<GcEvent> parse(List<String> lines) {
        // TODO
        return List.of();
    }

    /** True iff any line mentions OutOfMemoryError. */
    public static boolean sawOutOfMemory(List<String> lines) {
        // TODO
        return false;
    }

    /** True iff the live set (afterMb) trends upward toward the heap ceiling. */
    public static boolean isLeaking(List<GcEvent> events) {
        // TODO
        return false;
    }

    /** OLD_GEN_LEAK / HEALTHY_CHURN / UNKNOWN. */
    public static Diagnosis diagnose(List<GcEvent> events) {
        // TODO
        return Diagnosis.UNKNOWN;
    }
}
