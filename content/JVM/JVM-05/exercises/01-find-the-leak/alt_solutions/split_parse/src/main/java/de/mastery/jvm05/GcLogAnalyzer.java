package de.mastery.jvm05;

import java.util.ArrayList;
import java.util.List;

/** Alternate: hand tokenizing (no regex) + least-squares slope for the trend. */
public class GcLogAnalyzer {

    public record GcEvent(int id, long beforeMb, long afterMb, long totalMb, boolean full) {}

    public enum Diagnosis { OLD_GEN_LEAK, HEALTHY_CHURN, UNKNOWN }

    public static List<GcEvent> parse(List<String> lines) {
        List<GcEvent> out = new ArrayList<>();
        for (String line : lines) {
            int gcIdx = line.indexOf("GC(");
            int occIdx = line.indexOf("M->");
            if (gcIdx < 0 || occIdx < 0) {
                continue;
            }
            try {
                int idEnd = line.indexOf(')', gcIdx);
                int id = Integer.parseInt(line.substring(gcIdx + 3, idEnd));
                boolean full = line.contains("Pause Full");
                // "<before>M-><after>M(<total>M)"
                int beforeStart = line.lastIndexOf(' ', occIdx) + 1;
                long before = Long.parseLong(line.substring(beforeStart, occIdx));
                int afterStart = occIdx + 3;
                int afterEnd = line.indexOf('M', afterStart);
                long after = Long.parseLong(line.substring(afterStart, afterEnd));
                int totStart = line.indexOf('(', afterEnd) + 1;
                int totEnd = line.indexOf('M', totStart);
                long total = Long.parseLong(line.substring(totStart, totEnd));
                out.add(new GcEvent(id, before, after, total, full));
            } catch (RuntimeException ex) {
                // malformed -> skip
            }
        }
        return out;
    }

    public static boolean sawOutOfMemory(List<String> lines) {
        for (String l : lines) {
            if (l.contains("OutOfMemoryError")) {
                return true;
            }
        }
        return false;
    }

    public static boolean isLeaking(List<GcEvent> events) {
        if (events.size() < 6) {
            return false;
        }
        // positive least-squares slope of afterMb vs index, and final near ceiling
        int n = events.size();
        double meanX = (n - 1) / 2.0;
        double meanY = 0;
        for (GcEvent e : events) {
            meanY += e.afterMb();
        }
        meanY /= n;
        double num = 0;
        for (int i = 0; i < n; i++) {
            num += (i - meanX) * (events.get(i).afterMb() - meanY);
        }
        GcEvent last = events.get(n - 1);
        boolean strongUpSlope = num > 0 && last.afterMb() > 3 * events.get(0).afterMb();
        boolean nearCeiling = last.afterMb() >= 0.85 * last.totalMb();
        return strongUpSlope && nearCeiling;
    }

    public static Diagnosis diagnose(List<GcEvent> events) {
        if (events.isEmpty()) {
            return Diagnosis.UNKNOWN;
        }
        return isLeaking(events) ? Diagnosis.OLD_GEN_LEAK : Diagnosis.HEALTHY_CHURN;
    }
}
