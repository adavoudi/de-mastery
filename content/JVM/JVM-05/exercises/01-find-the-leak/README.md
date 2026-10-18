# Read a heap/GC log and find the leak

A streaming job's JVM died. You have its unified GC log (`-Xlog:gc*`). Your job is to write a small
analyzer that parses the log, tracks the **live set** (heap used *after* each GC), and decides
whether the process was **leaking** — exactly the post-mortem you do when a Flink TaskManager or
Spark executor OOMs.

Two committed log artifacts are provided (in `artifact/`, and the test feeds you identical lines):

- `gc-leak.log` — a run whose post-GC occupancy climbs toward the 1024M ceiling, with Full GCs that
  reclaim less and less, ending in `OutOfMemoryError`.
- `gc-healthy.log` — a run with a flat post-GC floor (~120M) and only minor GCs (healthy churn).

Implement `src/main/java/de/mastery/jvm05/GcLogAnalyzer.java` (package `de.mastery.jvm05`). The file
provides a `record GcEvent(int id, long beforeMb, long afterMb, long totalMb, boolean full)` and an
`enum Diagnosis { OLD_GEN_LEAK, HEALTHY_CHURN, UNKNOWN }`.

1. `List<GcEvent> parse(List<String> lines)` — parse each GC pause line of the form
   `... GC(<id>) Pause <Young|Full> ... <before>M-><after>M(<total>M) <pause>ms`. Capture `id`,
   `beforeMb`, `afterMb`, `totalMb`, and `full = true` for `Pause Full`. **Skip** any line that is
   not a GC pause line (e.g. the `OutOfMemoryError` line, which has no occupancy numbers). Preserve
   order.

2. `boolean sawOutOfMemory(List<String> lines)` — true iff any line contains `OutOfMemoryError`.

3. `boolean isLeaking(List<GcEvent> events)` — true iff the live set (the `afterMb` values) trends
   clearly **upward** over the run AND the final live set is near the heap ceiling (reclaiming
   little). It must return **false** for the healthy log (flat floor). (A simple, robust rule: the
   mean of the last three `afterMb` is at least 2× the mean of the first three, and the last
   `afterMb` is ≥ 85% of `totalMb`.)

4. `Diagnosis diagnose(List<GcEvent> events)` — `OLD_GEN_LEAK` when `isLeaking` is true,
   `HEALTHY_CHURN` when there are events but it is not leaking, `UNKNOWN` when there are no events.

Constraints:
- Do not change the record, the enum, or the signatures. Keep the package.
- Base the decision on the parsed `afterMb` trend, not on the pause times. The test checks your
  parser on exact numbers and your classifier on both logs (and a tiny empty case).
