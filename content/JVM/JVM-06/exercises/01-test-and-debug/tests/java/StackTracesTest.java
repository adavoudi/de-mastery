import de.mastery.jvm06.StackTraces;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StackTracesTest {

    private static final String TRACE =
            "java.lang.RuntimeException: pipeline failed\n" +
            "\tat de.mastery.job.Pipeline.run(Pipeline.java:42)\n" +
            "\tat de.mastery.job.Main.main(Main.java:11)\n" +
            "Caused by: java.lang.IllegalStateException: bad state\n" +
            "\tat de.mastery.job.Pipeline.step(Pipeline.java:70)\n" +
            "\t... 2 more\n" +
            "Caused by: java.lang.NumberFormatException: For input string: \"N/A\"\n" +
            "\tat java.base/java.lang.Long.parseLong(Long.java:711)\n" +
            "\tat de.mastery.job.Parser.toLong(Parser.java:88)\n" +
            "\t... 3 more\n";

    @Test
    void rootCauseIsDeepestCausedBy() {
        assertThat(StackTraces.rootCause(TRACE))
                .isEqualTo("java.lang.NumberFormatException");
    }

    @Test
    void rootCauseWithNoCausedByIsTopException() {
        String t = "java.lang.NullPointerException: boom\n\tat de.x.A.b(A.java:3)\n";
        assertThat(StackTraces.rootCause(t)).isEqualTo("java.lang.NullPointerException");
    }

    @Test
    void topAppFrameSkipsJdkFrames() {
        // The first app frame UNDER the root cause is Parser.toLong; java.base frame is skipped,
        // but note the method returns the first app frame in the WHOLE trace order.
        assertThat(StackTraces.topAppFrame(TRACE, "de.mastery.job.Parser"))
                .isEqualTo("de.mastery.job.Parser.toLong(Parser.java:88)");
    }

    @Test
    void topAppFrameByPackagePrefix() {
        // First frame whose class starts with the given prefix.
        assertThat(StackTraces.topAppFrame(TRACE, "de.mastery.job"))
                .isEqualTo("de.mastery.job.Pipeline.run(Pipeline.java:42)");
    }

    @Test
    void topAppFrameReturnsNullWhenNoMatch() {
        assertThat(StackTraces.topAppFrame(TRACE, "com.other")).isNull();
    }
}
