package de.mastery.jvm06;

/** Alternate: index-based string scanning. */
public class StackTraces {

    public static String rootCause(String trace) {
        if (trace == null || trace.isBlank()) {
            return null;
        }
        int idx = trace.lastIndexOf("Caused by:");
        String line;
        if (idx >= 0) {
            line = sliceLine(trace, idx + "Caused by:".length());
        } else {
            // first non-blank line
            int start = 0;
            while (start < trace.length() && Character.isWhitespace(trace.charAt(start))) {
                start++;
            }
            line = sliceLine(trace, start);
        }
        line = line.strip();
        int colon = line.indexOf(':');
        return colon >= 0 ? line.substring(0, colon).strip() : line;
    }

    public static String topAppFrame(String trace, String packagePrefix) {
        if (trace == null) {
            return null;
        }
        for (String raw : trace.split("\\R")) {
            String t = raw.strip();
            if (t.startsWith("at ")) {
                String frame = t.substring(3).strip();
                if (frame.startsWith(packagePrefix)) {
                    return frame;
                }
            }
        }
        return null;
    }

    private static String sliceLine(String s, int from) {
        int end = s.indexOf('\n', from);
        if (end < 0) {
            end = s.length();
        }
        return s.substring(from, end);
    }
}
