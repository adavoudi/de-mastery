# Read a heap/GC log and find the leak

The JVM of a streaming job died. You have its unified GC log (`-Xlog:gc*`). Your job is to write a
small analyzer. It parses the log and tracks the **live set** (the heap used *after* each GC). Then
it decides whether the process was **leaking**. This is exactly the post-mortem that you do when a
Flink TaskManager or a Spark executor OOMs.

The task provides two committed log artifacts (in `artifact/`). The test gives you identical lines.

- `gc-leak.log` — a run in which the post-GC occupancy climbs toward the 1024M ceiling. Full GCs
  reclaim less and less. The run ends in `OutOfMemoryError`.
- `gc-healthy.log` — a run with a flat post-GC floor (~120M) and only minor GCs (healthy churn).

Implement `src/main/java/de/mastery/jvm05/GcLogAnalyzer.java` (package `de.mastery.jvm05`). The file
provides a `record GcEvent(int id, long beforeMb, long afterMb, long totalMb, boolean full)` and an
`enum Diagnosis { OLD_GEN_LEAK, HEALTHY_CHURN, UNKNOWN }`.

1. `List<GcEvent> parse(List<String> lines)` — parse each GC pause line of the form
   `... GC(<id>) Pause <Young|Full> ... <before>M-><after>M(<total>M) <pause>ms`. Capture `id`,
   `beforeMb`, `afterMb` and `totalMb`. Set `full = true` for `Pause Full`. **Skip** any line that
   is not a GC pause line (for example the `OutOfMemoryError` line, which has no occupancy
   numbers). Keep the order.

2. `boolean sawOutOfMemory(List<String> lines)` — return true if and only if any line contains
   `OutOfMemoryError`.

3. `boolean isLeaking(List<GcEvent> events)` — return true if and only if two things are true. The
   live set (the `afterMb` values) trends clearly **upward** over the run. The final live set is
   also near the heap ceiling (it reclaims little). The method must return **false** for the
   healthy log (flat floor). (A simple rule that works: the mean of the last three `afterMb` is at
   least 2× the mean of the first three. Also, the last `afterMb` is ≥ 85% of `totalMb`.)

4. `Diagnosis diagnose(List<GcEvent> events)` — return `OLD_GEN_LEAK` when `isLeaking` is true.
   Return `HEALTHY_CHURN` when there are events but it is not leaking. Return `UNKNOWN` when there
   are no events.

Constraints:
- Do not change the record, the enum, or the signatures. Keep the package.
- Base the decision on the parsed `afterMb` trend, not on the pause times. The test checks your
  parser on exact numbers. It also checks your classifier on both logs (and a tiny empty case).
