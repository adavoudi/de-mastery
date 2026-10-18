# Survive a TaskManager crash with exactly-once ledger invariants

Write a Flink streaming job that processes a ledger of `N` entries and writes the id of each entry to
an exactly-once sink. The grader runs your job on a real cluster and **sends SIGKILL to a
TaskManager in the middle of the run**. The job recovers from its last checkpoint. Then the grader
asserts the **ledger invariants** over the *committed* output: every id appears **exactly once**.
There is no loss and no duplication.

Implement `de.flk.LedgerJob` with a `main`:

```java
public static void main(String[] args) throws Exception
```

- `args[0]` is the **output directory URI** (for example `file:///tmp/....`). Write committed files
  there.
- The source is the bounded, replayable sequence `0 .. N-1`. `N` is read from `args[1]`
  (use `NumberSequenceSource(0, N-1)`).

Your job must do these steps:

1. **Enable checkpointing** with `env.enableCheckpointing(intervalMs)`. Keep the interval small (for
   example a few hundred ms), so that a checkpoint completes before the crash. The default
   checkpointing mode is EXACTLY_ONCE. Flink snapshots keyed state and source offsets together.
2. Build the pipeline from the `NumberSequenceSource`. Key the stream (for example by `id % 4`). Run
   it through a small `KeyedProcessFunction` that simulates work for each record (a tiny
   `Thread.sleep`). Then the job is still running when the grader kills the TaskManager. Emit each id
   as its decimal string.
3. **Write with a commit-on-checkpoint sink**: a `FileSink.forRowFormat(new Path(args[0]), new
   SimpleStringEncoder<String>("UTF-8"))` with `.withRollingPolicy(OnCheckpointRollingPolicy.build())`.
   Each part file becomes visible only when the checkpoint that produced it completes. So Flink rolls
   back the output produced since the last complete checkpoint on restart (no duplicates).
4. Call `env.execute(...)`.

After the job finishes, the committed output (the non-hidden part files) must contain each id in
`0 .. N-1` **exactly once**. This must be true even though the grader killed a TaskManager while the
job was running.

### Why the starter fails

The starter enables no checkpointing. It uses a sink with no commit-on-checkpoint policy. So after
a crash, the output either loses records that were in flight or emits them again. The invariants
break. Add the three ingredients above: the replayable source (already given), checkpointed state,
and a committing sink.
