"""FLK-11 — fix a backpressured job. Graded on the JOB GRAPH (deterministic),
not wall-clock.

A generated driver builds (a) the baseline bottleneck (heavy map at parallelism
1, FORWARD edge) and (b) the learner's `de.flk.Fix.apply(...)`, extracts both
execution plans and both summed outputs, and prints them as JSON. The test then
asserts the learner's "heavy" operator parallelism increased, the edge feeding it
is a redistributing strategy (not FORWARD), and the output is unchanged (G6: the
same sum, cross-checked against an independent Python/DuckDB fold over Work::busy).
"""

import duckdb

N = 2000

_DRIVER = r'''
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

public class DeDriver {
  static long N = %d;

  static String baselinePlan() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Long> s = env.fromSequence(1, N).setParallelism(1);
    s.map((MapFunction<Long,Long>) de.flk.Work::busy).name("heavy").setParallelism(1).print();
    return env.getExecutionPlan();
  }
  static String learnerPlan() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Long> s = env.fromSequence(1, N).setParallelism(1);
    de.flk.Fix.apply(s).print();
    return env.getExecutionPlan();
  }
  static long baselineSum() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Long> s = env.fromSequence(1, N).setParallelism(1);
    long t=0; CloseableIterator<Long> it =
      s.map((MapFunction<Long,Long>) de.flk.Work::busy).setParallelism(1).executeAndCollect();
    while(it.hasNext()) t += it.next(); it.close(); return t;
  }
  static long learnerSum() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Long> s = env.fromSequence(1, N).setParallelism(1);
    long t=0; CloseableIterator<Long> it = de.flk.Fix.apply(s).executeAndCollect();
    while(it.hasNext()) t += it.next(); it.close(); return t;
  }
  static String j(String s){ StringBuilder b=new StringBuilder("\""); for(char c:s.toCharArray()){ if(c=='"'||c=='\\')b.append('\\').append(c); else b.append(c);} return b.append('"').toString(); }

  public static void main(String[] a) throws Exception {
    String bp = baselinePlan().replaceAll("\\s+"," ");
    String lp = learnerPlan().replaceAll("\\s+"," ");
    long bs = baselineSum(), ls = learnerSum();
    System.out.println("DE_RESULT={\"baseline_plan\":"+j(bp)+",\"learner_plan\":"+j(lp)
        +",\"baseline_sum\":"+bs+",\"learner_sum\":"+ls+"}");
  }
}
''' % N

# The heavy map is NON-keyed, so the correct redistribution is an even one
# (REBALANCE/RESCALE). HASH is deliberately NOT accepted: a keyBy on this
# non-keyed stream (e.g. keyBy a constant) funnels every record to a single
# subtask — the very bottleneck we are removing — so it must be rejected.
REDISTRIBUTING = {"REBALANCE", "RESCALE"}


def _heavy(plan_json):
    """Return (parallelism, feeding_ship_strategies) for the 'heavy' operator."""
    import json
    p = json.loads(plan_json)
    for n in p["nodes"]:
        if "heavy" in n.get("contents", ""):
            edges = [pr["ship_strategy"].upper() for pr in n.get("predecessors", [])]
            return n["parallelism"], edges
    return None, []


def _run(flink_java, submission_dir):
    r = flink_java(submission_dir, _DRIVER)
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2000:]}"
    assert isinstance(r.result, dict), f"bad DE_RESULT: {r.result}\n{r.stdout[-500:]}"
    return r.result


def test_plan_changed_parallelism(flink_java, submission_dir):
    res = _run(flink_java, submission_dir)
    b_par, _ = _heavy(res["baseline_plan"])
    l_par, _ = _heavy(res["learner_plan"])
    assert b_par == 1, f"baseline heavy parallelism should be 1, got {b_par}"
    assert l_par is not None and l_par > b_par, \
        f"learner must raise the heavy operator's parallelism above {b_par}; got {l_par}"


def test_plan_changed_redistribution(flink_java, submission_dir):
    res = _run(flink_java, submission_dir)
    _, b_edges = _heavy(res["baseline_plan"])
    _, l_edges = _heavy(res["learner_plan"])
    assert b_edges == ["FORWARD"], f"baseline edge should be FORWARD, got {b_edges}"
    assert l_edges, "learner heavy operator has no feeding edge"
    assert all(e in REDISTRIBUTING for e in l_edges), \
        (f"the edge feeding the heavy operator must redistribute "
         f"evenly (REBALANCE/RESCALE; HASH/keyBy is rejected), not {l_edges}")


def test_result_unchanged(flink_java, submission_dir):
    res = _run(flink_java, submission_dir)
    assert res["learner_sum"] == res["baseline_sum"], \
        "the optimization must not change the output (same summed result)"
    # G6 oracle: independent fold over the same Work::busy computation
    con = duckdb.connect()
    expected = con.execute(
        "SELECT SUM((SELECT SUM((x*2654435761 + i) %% 97) FROM range(0,2000) AS r(i)) %% 1000) "
        "FROM range(1, %d + 1) AS t(x)" % N).fetchone()[0]
    assert res["baseline_sum"] == expected, \
        f"baseline sum {res['baseline_sum']} != oracle {expected}"
