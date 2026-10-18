package de.mastery.jvm06;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Alternate: regex-driven parsing. */
public class StackTraces {

    private static final Pattern CAUSED = Pattern.compile("^Caused by:\\s*([\\w.$]+)");
    private static final Pattern FIRST = Pattern.compile("^([\\w.$]+)");
    private static final Pattern FRAME = Pattern.compile("^at\\s+(.+)$");

    public static String rootCause(String trace) {
        if (trace == null || trace.isBlank()) {
            return null;
        }
        String root = null;
        for (String line : trace.split("\\R")) {
            Matcher m = CAUSED.matcher(line.strip());
            if (m.find()) {
                root = m.group(1);
            }
        }
        if (root == null) {
            for (String line : trace.split("\\R")) {
                String t = line.strip();
                if (!t.isEmpty()) {
                    Matcher m = FIRST.matcher(t);
                    if (m.find()) {
                        root = m.group(1);
                    }
                    break;
                }
            }
        }
        return root;
    }

    public static String topAppFrame(String trace, String packagePrefix) {
        if (trace == null) {
            return null;
        }
        for (String line : trace.split("\\R")) {
            Matcher m = FRAME.matcher(line.strip());
            if (m.find()) {
                String frame = m.group(1).strip();
                if (frame.startsWith(packagePrefix)) {
                    return frame;
                }
            }
        }
        return null;
    }
}
