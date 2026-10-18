import de.mastery.jvm05.GcLogAnalyzer;
import de.mastery.jvm05.GcLogAnalyzer.Diagnosis;
import de.mastery.jvm05.GcLogAnalyzer.GcEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GcLogAnalyzerTest {

    // Identical to artifact/gc-leak.log (committed) — a rising live set ending in OOM.
    private static final List<String> LEAK = List.of(
            "[0.512s][info][gc] GC(0) Pause Young (G1 Evacuation Pause) 120M->45M(1024M) 6.2ms",
            "[2.004s][info][gc] GC(1) Pause Young (G1 Evacuation Pause) 300M->95M(1024M) 7.1ms",
            "[3.880s][info][gc] GC(2) Pause Young (G1 Evacuation Pause) 410M->150M(1024M) 7.9ms",
            "[5.903s][info][gc] GC(3) Pause Young (G1 Evacuation Pause) 520M->210M(1024M) 8.4ms",
            "[8.110s][info][gc] GC(4) Pause Young (G1 Evacuation Pause) 640M->280M(1024M) 9.0ms",
            "[10.774s][info][gc] GC(5) Pause Young (G1 Evacuation Pause) 760M->360M(1024M) 10.2ms",
            "[13.900s][info][gc] GC(6) Pause Young (G1 Evacuation Pause) 840M->450M(1024M) 11.6ms",
            "[16.550s][info][gc] GC(7) Pause Full (G1 Compaction Pause) 980M->560M(1024M) 95.4ms",
            "[19.980s][info][gc] GC(8) Pause Young (G1 Evacuation Pause) 900M->650M(1024M) 12.9ms",
            "[23.110s][info][gc] GC(9) Pause Full (G1 Compaction Pause) 1000M->760M(1024M) 142.7ms",
            "[26.700s][info][gc] GC(10) Pause Young (G1 Evacuation Pause) 980M->840M(1024M) 15.1ms",
            "[30.220s][info][gc] GC(11) Pause Full (G1 Compaction Pause) 1012M->910M(1024M) 188.3ms",
            "[33.410s][info][gc] GC(12) Pause Full (G1 Compaction Pause) 1018M->980M(1024M) 221.6ms",
            "[35.902s][info][gc] GC(13) Pause Full (G1 Compaction Pause) 1021M->1012M(1024M) 318.9ms",
            "[36.550s][info][gc] java.lang.OutOfMemoryError: Java heap space");

    // Identical to artifact/gc-healthy.log — a flat ~120M floor, minor GCs only.
    private static final List<String> HEALTHY = List.of(
            "[0.480s][info][gc] GC(0) Pause Young (G1 Evacuation Pause) 410M->120M(1024M) 6.0ms",
            "[1.900s][info][gc] GC(1) Pause Young (G1 Evacuation Pause) 520M->130M(1024M) 6.3ms",
            "[3.420s][info][gc] GC(2) Pause Young (G1 Evacuation Pause) 505M->118M(1024M) 6.1ms",
            "[4.950s][info][gc] GC(3) Pause Young (G1 Evacuation Pause) 530M->125M(1024M) 6.4ms",
            "[6.510s][info][gc] GC(4) Pause Young (G1 Evacuation Pause) 515M->122M(1024M) 6.2ms",
            "[8.090s][info][gc] GC(5) Pause Young (G1 Evacuation Pause) 528M->119M(1024M) 6.3ms",
            "[9.700s][info][gc] GC(6) Pause Young (G1 Evacuation Pause) 540M->128M(1024M) 6.5ms",
            "[11.330s][info][gc] GC(7) Pause Young (G1 Evacuation Pause) 522M->121M(1024M) 6.2ms",
            "[12.990s][info][gc] GC(8) Pause Young (G1 Evacuation Pause) 535M->124M(1024M) 6.4ms",
            "[14.670s][info][gc] GC(9) Pause Young (G1 Evacuation Pause) 518M->120M(1024M) 6.1ms");

    @Test
    void parsesLeakLogExactly() {
        List<GcEvent> events = GcLogAnalyzer.parse(LEAK);
        assertThat(events).hasSize(14);   // 14 GC lines, OOM line skipped
        GcEvent first = events.get(0);
        assertThat(first.id()).isEqualTo(0);
        assertThat(first.beforeMb()).isEqualTo(120L);
        assertThat(first.afterMb()).isEqualTo(45L);
        assertThat(first.totalMb()).isEqualTo(1024L);
        assertThat(first.full()).isFalse();
        GcEvent full7 = events.get(7);
        assertThat(full7.id()).isEqualTo(7);
        assertThat(full7.full()).isTrue();
        assertThat(full7.afterMb()).isEqualTo(560L);
        assertThat(events.get(13).afterMb()).isEqualTo(1012L);
    }

    @Test
    void skipsNonGcLines() {
        // The OOM line must not become an event.
        assertThat(GcLogAnalyzer.parse(List.of(
                "[36.550s][info][gc] java.lang.OutOfMemoryError: Java heap space"))).isEmpty();
    }

    @Test
    void detectsOutOfMemory() {
        assertThat(GcLogAnalyzer.sawOutOfMemory(LEAK)).isTrue();
        assertThat(GcLogAnalyzer.sawOutOfMemory(HEALTHY)).isFalse();
    }

    @Test
    void leakLogIsLeaking() {
        assertThat(GcLogAnalyzer.isLeaking(GcLogAnalyzer.parse(LEAK))).isTrue();
    }

    @Test
    void healthyLogIsNotLeaking() {
        assertThat(GcLogAnalyzer.isLeaking(GcLogAnalyzer.parse(HEALTHY))).isFalse();
    }

    @Test
    void diagnoseClassifiesBothLogs() {
        assertThat(GcLogAnalyzer.diagnose(GcLogAnalyzer.parse(LEAK)))
                .isEqualTo(Diagnosis.OLD_GEN_LEAK);
        assertThat(GcLogAnalyzer.diagnose(GcLogAnalyzer.parse(HEALTHY)))
                .isEqualTo(Diagnosis.HEALTHY_CHURN);
        assertThat(GcLogAnalyzer.diagnose(List.of())).isEqualTo(Diagnosis.UNKNOWN);
    }
}
