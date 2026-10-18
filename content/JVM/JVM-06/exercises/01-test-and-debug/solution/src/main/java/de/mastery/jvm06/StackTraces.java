package de.mastery.jvm06;

/** Reference. */
public class StackTraces {

    public static String rootCause(String trace) {
        if (trace == null || trace.isBlank()) {
            return null;
        }
        String[] lines = trace.split("\\R");
        String rootLine = null;
        for (String line : lines) {
            String t = line.strip();
            if (t.startsWith("Caused by:")) {
                rootLine = t.substring("Caused by:".length()).strip();
            }
        }
        if (rootLine == null) {
            // no Caused by: use the first non-blank line (the thrown exception)
            for (String line : lines) {
                if (!line.isBlank()) {
                    rootLine = line.strip();
                    break;
                }
            }
        }
        if (rootLine == null) {
            return null;
        }
        // rootLine looks like "fqcn: message" or just "fqcn"
        int colon = rootLine.indexOf(':');
        return colon >= 0 ? rootLine.substring(0, colon).strip() : rootLine;
    }

    public static String topAppFrame(String trace, String packagePrefix) {
        if (trace == null) {
            return null;
        }
        for (String line : trace.split("\\R")) {
            String t = line.strip();
            if (!t.startsWith("at ")) {
                continue;
            }
            String frame = t.substring(3).strip();   // e.g. de.mastery.job.Parser.toLong(Parser.java:88)
            if (frame.startsWith(packagePrefix)) {
                return frame;
            }
        }
        return null;
    }
}
