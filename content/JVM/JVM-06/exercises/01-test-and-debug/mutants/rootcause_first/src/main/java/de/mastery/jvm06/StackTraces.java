package de.mastery.jvm06;

public class StackTraces {

    public static String rootCause(String trace) {
        if (trace == null || trace.isBlank()) {
            return null;
        }
        // BUG: returns the FIRST Caused by (a wrapper), not the deepest root cause.
        for (String line : trace.split("\\R")) {
            String t = line.strip();
            if (t.startsWith("Caused by:")) {
                String rest = t.substring("Caused by:".length()).strip();
                int colon = rest.indexOf(':');
                return colon >= 0 ? rest.substring(0, colon).strip() : rest;
            }
        }
        String first = trace.strip().split("\\R")[0].strip();
        int colon = first.indexOf(':');
        return colon >= 0 ? first.substring(0, colon).strip() : first;
    }

    public static String topAppFrame(String trace, String packagePrefix) {
        if (trace == null) {
            return null;
        }
        for (String line : trace.split("\\R")) {
            String t = line.strip();
            if (t.startsWith("at ")) {
                String frame = t.substring(3).strip();
                if (frame.startsWith(packagePrefix)) {
                    return frame;
                }
            }
        }
        return null;
    }
}
