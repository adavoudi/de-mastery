"""FLK-01 — job graph & slot demand (embedded, no Docker).

The learner builds a keyed pipeline (source -> filter -> keyBy -> sum -> sink) at
a requested parallelism. A generated driver calls ``de.flk.JobShape.plan(p)`` and
prints the report as ``DE_RESULT=<json>`` with the raw JobGraph JSON plus the
job-wide parallelism and the number of task slots the job needs.

We assert, with an INDEPENDENT oracle (G6):
  * job_parallelism == requested p         (starter returns 0 -> G1)
  * slots_needed == p  (MAX operator parallelism under default slot sharing,
    NOT the sum of operator parallelisms)
  * the plan contains a HASH exchange (a keyBy breaks the chain)
  * the keyed-aggregation vertex runs at parallelism p

SKIPs cleanly when the Flink dist / JDK are absent.
"""

import json

# The driver reads the parallelism from an env var so the same driver serves
# multiple cases without recompiling the harness logic in Python.
_DRIVER = r"""
import de.flk.JobShape;

public class DeDriver {
  public static void main(String[] a) throws Exception {
    int p = Integer.parseInt(System.getenv().getOrDefault("PAR", "4"));
    JobShape.PlanReport r = JobShape.plan(p);
    // emit {"job_parallelism":..,"slots_needed":..,"plan": "<escaped json>"}
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"job_parallelism\":").append(r.jobParallelism).append(",");
    sb.append("\"slots_needed\":").append(r.slotsNeeded).append(",");
    sb.append("\"plan\":").append(quote(r.plan));
    sb.append("}");
    System.out.println("DE_RESULT=" + sb);
  }

  static String quote(String s) {
    StringBuilder b = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"': b.append("\\\""); break;
        case '\\': b.append("\\\\"); break;
        case '\n': b.append("\\n"); break;
        case '\r': b.append("\\r"); break;
        case '\t': b.append("\\t"); break;
        default: b.append(c);
      }
    }
    return b.append("\"").toString();
  }
}
"""


def _run(flink_java, submission_dir, parallelism):
    r = flink_java(submission_dir, _DRIVER, env_vars={"PAR": str(parallelism)})
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2000:]}"
    assert isinstance(r.result, dict), f"bad result {r.result!r}"
    return r.result


def test_job_parallelism_is_set(flink_java, submission_dir):
    out = _run(flink_java, submission_dir, 4)
    assert out["job_parallelism"] == 4, out


def test_slots_needed_is_max_not_sum(flink_java, submission_dir):
    # Under default slot sharing a linear pipeline needs slots == MAX operator
    # parallelism, i.e. the job parallelism, NOT the sum across operators.
    for p in (2, 4):
        out = _run(flink_java, submission_dir, p)
        assert out["slots_needed"] == p, (p, out["slots_needed"])


def test_plan_has_keyby_hash_exchange(flink_java, submission_dir):
    out = _run(flink_java, submission_dir, 4)
    plan = json.loads(out["plan"])
    nodes = plan["nodes"]
    strategies = {
        pre.get("ship_strategy")
        for n in nodes
        for pre in n.get("predecessors", [])
    }
    # a keyBy produces a HASH ship strategy between the filter and the aggregation
    assert "HASH" in strategies, f"no keyBy/HASH exchange in plan: {strategies}"


def test_keyed_operator_parallelism(flink_java, submission_dir):
    # the operator on the DOWNSTREAM side of the HASH (keyBy) edge is the keyed
    # aggregation/reduce; it must run at the requested parallelism. We locate it
    # structurally (via the HASH edge) rather than by a type-name string so that
    # sum() ("Keyed Aggregation") and reduce() ("Keyed Reduce") both qualify.
    out = _run(flink_java, submission_dir, 4)
    plan = json.loads(out["plan"])
    nodes = plan["nodes"]
    keyed = [
        n
        for n in nodes
        if any(pre.get("ship_strategy") == "HASH"
               for pre in n.get("predecessors", []))
    ]
    assert keyed, f"no node downstream of a HASH exchange: {[n.get('type') for n in nodes]}"
    assert all(n["parallelism"] == 4 for n in keyed), keyed
