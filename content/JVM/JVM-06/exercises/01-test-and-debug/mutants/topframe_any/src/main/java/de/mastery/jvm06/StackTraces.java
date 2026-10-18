package de.mastery.jvm06;

public class StackTraces {

    public static String rootCause(String trace) {
        if (trace == null || trace.isBlank()) {
            return null;
        }
        String rootLine = null;
        for (String line : trace.split("\\R")) {
            String t = line.strip();
            if (t.startsWith("Caused by:")) {
                rootLine = t.substring("Caused by:".length()).strip();
            }
        }
        if (rootLine == null) {
            for (String line : trace.split("\\R")) {
                if (!line.isBlank()) {
                    rootLine = line.strip();
                    break;
                }
            }
        }
        if (rootLine == null) {
            return null;
        }
        int colon = rootLine.indexOf(':');
        return colon >= 0 ? rootLine.substring(0, colon).strip() : rootLine;
    }

    public static String topAppFrame(String trace, String packagePrefix) {
        if (trace == null) {
            return null;
        }
        for (String line : trace.split("\\R")) {
            String t = line.strip();
            if (t.startsWith("at ")) {
                // BUG: ignores the package prefix, returning the first frame (even JDK/no-match).
                return t.substring(3).strip();
            }
        }
        return null;
    }
}
