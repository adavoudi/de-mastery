package de.mastery.jvm05;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GcLogAnalyzer {

    public record GcEvent(int id, long beforeMb, long afterMb, long totalMb, boolean full) {}

    public enum Diagnosis { OLD_GEN_LEAK, HEALTHY_CHURN, UNKNOWN }

    private static final Pattern LINE = Pattern.compile(
            "GC\\((\\d+)\\)\\s+Pause\\s+(\\w+).*?(\\d+)M->(\\d+)M\\((\\d+)M\\)");

    public static List<GcEvent> parse(List<String> lines) {
        List<GcEvent> out = new ArrayList<>();
        for (String line : lines) {
            Matcher m = LINE.matcher(line);
            if (!m.find()) {
                continue;
            }
            out.add(new GcEvent(
                    Integer.parseInt(m.group(1)),
                    Long.parseLong(m.group(3)),
                    Long.parseLong(m.group(4)),
                    Long.parseLong(m.group(5)),
                    m.group(2).equalsIgnoreCase("Full")));
        }
        return out;
    }

    public static boolean sawOutOfMemory(List<String> lines) {
        return lines.stream().anyMatch(l -> l.contains("OutOfMemoryError"));
    }

    public static boolean isLeaking(List<GcEvent> events) {
        if (events.size() < 6) {
            return false;
        }
        double firstMean = meanAfter(events, 0, 3);
        double lastMean = meanAfter(events, events.size() - 3, events.size());
        GcEvent last = events.get(events.size() - 1);
        return lastMean >= 2.0 * firstMean && last.afterMb() >= 0.85 * last.totalMb();
    }

    public static Diagnosis diagnose(List<GcEvent> events) {
        if (events.isEmpty()) {
            return Diagnosis.UNKNOWN;
        }
        // BUG: never reports a leak; always classifies as healthy churn.
        return Diagnosis.HEALTHY_CHURN;
    }

    private static double meanAfter(List<GcEvent> events, int from, int to) {
        double sum = 0;
        for (int i = from; i < to; i++) {
            sum += events.get(i).afterMb();
        }
        return sum / (to - from);
    }
}
