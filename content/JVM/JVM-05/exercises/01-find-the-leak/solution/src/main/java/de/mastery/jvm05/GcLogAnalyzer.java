package de.mastery.jvm05;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Reference. */
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
                continue;   // not a GC pause line (e.g. the OOM line)
            }
            int id = Integer.parseInt(m.group(1));
            boolean full = m.group(2).equalsIgnoreCase("Full");
            long before = Long.parseLong(m.group(3));
            long after = Long.parseLong(m.group(4));
            long total = Long.parseLong(m.group(5));
            out.add(new GcEvent(id, before, after, total, full));
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
        boolean risingTrend = lastMean >= 2.0 * firstMean;
        boolean nearCeiling = last.afterMb() >= 0.85 * last.totalMb();
        return risingTrend && nearCeiling;
    }

    public static Diagnosis diagnose(List<GcEvent> events) {
        if (events.isEmpty()) {
            return Diagnosis.UNKNOWN;
        }
        return isLeaking(events) ? Diagnosis.OLD_GEN_LEAK : Diagnosis.HEALTHY_CHURN;
    }

    private static double meanAfter(List<GcEvent> events, int from, int to) {
        double sum = 0;
        for (int i = from; i < to; i++) {
            sum += events.get(i).afterMb();
        }
        return sum / (to - from);
    }
}
