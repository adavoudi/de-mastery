package de.mastery.jvm05;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Alternate: single combined regex + a min-in-first-half / max-near-end leak rule. */
public class GcLogAnalyzer {

    public record GcEvent(int id, long beforeMb, long afterMb, long totalMb, boolean full) {}

    public enum Diagnosis { OLD_GEN_LEAK, HEALTHY_CHURN, UNKNOWN }

    private static final Pattern P = Pattern.compile(
            "GC\\((\\d+)\\)\\s+Pause\\s+(Young|Full)\\b.*?(\\d+)M->(\\d+)M\\((\\d+)M\\)");

    public static List<GcEvent> parse(List<String> lines) {
        List<GcEvent> out = new ArrayList<>();
        for (String line : lines) {
            Matcher m = P.matcher(line);
            if (m.find()) {
                out.add(new GcEvent(
                        Integer.parseInt(m.group(1)),
                        Long.parseLong(m.group(3)),
                        Long.parseLong(m.group(4)),
                        Long.parseLong(m.group(5)),
                        m.group(2).equals("Full")));
            }
        }
        return out;
    }

    public static boolean sawOutOfMemory(List<String> lines) {
        return lines.stream().anyMatch(l -> l.contains("OutOfMemoryError"));
    }

    public static boolean isLeaking(List<GcEvent> events) {
        int n = events.size();
        if (n < 6) {
            return false;
        }
        long firstAfter = events.get(0).afterMb();
        long lastAfter = events.get(n - 1).afterMb();
        long total = events.get(n - 1).totalMb();
        // live set at the end dwarfs the start and sits near the ceiling
        boolean grew = lastAfter >= firstAfter + total / 2;
        boolean nearCeiling = lastAfter * 100 >= total * 85;
        return grew && nearCeiling;
    }

    public static Diagnosis diagnose(List<GcEvent> events) {
        if (events.isEmpty()) {
            return Diagnosis.UNKNOWN;
        }
        return isLeaking(events) ? Diagnosis.OLD_GEN_LEAK : Diagnosis.HEALTHY_CHURN;
    }
}
