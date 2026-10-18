package de.flk;

/** Provided CPU-heavy per-record computation. Do not change its logic — both
 *  the baseline and your Fix call Work::busy, so the comparison is fair and the
 *  result (the summed output) must be identical. */
public final class Work {
    private Work() {}

    public static long busy(long x) {
        long s = 0;
        for (int i = 0; i < 2000; i++) {
            s += (x * 2654435761L + i) % 97;
        }
        return s % 1000;
    }
}
