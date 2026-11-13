package io.github.amwtke.pipeliner.runtime.fake;

import io.github.amwtke.pipeliner.core.graph.*;
import io.github.amwtke.pipeliner.core.plan.*;
import java.util.*;

/** Minimal planner for tests (one run per stream-connected region, no merging, no auto gates). */
public final class TestPlanner {
    private TestPlanner() {}

    /** @param modes pipeline id -> mode of its run (default CLUSTER) */
    public static ExecutionPlan plan(PipelineGraph g, Map<String, ExecMode> modes) {
        List<String> ids = new ArrayList<>(g.getPipelines().keySet());
        Map<String, String> parent = new HashMap<>();
        for (String id : ids) parent.put(id, id);
        for (EdgeSpec e : g.getEdges()) parent.put(find(parent, e.getFromPipeline()), find(parent, e.getToPipeline()));
        Map<String, List<String>> regions = new LinkedHashMap<>();
        for (String id : ids) {
            List<String> l = regions.get(find(parent, id));
            if (l == null) { l = new ArrayList<>(); regions.put(find(parent, id), l); }
            l.add(id);
        }
        Map<String, String> runOf = new HashMap<>();
        List<List<String>> regionList = new ArrayList<>();
        int n = 0;
        for (List<String> members : regions.values()) {
            n++;
            for (String m : members) runOf.put(m, "R" + n);
            regionList.add(topo(g, members));
        }
        List<PlanChannel> channels = new ArrayList<>();
        int ord = 0;
        for (EdgeSpec e : g.getEdges()) {
            ord++;
            channels.add(PlanChannel.builder().edgeId("e" + ord).ordinal(ord).fromPipeline(e.getFromPipeline()).fromPort(e.getFromPort())
                    .toPipeline(e.getToPipeline()).runId(runOf.get(e.getFromPipeline())).build());
        }
        List<PlanGate> gates = new ArrayList<>();
        for (GateSpec gs : g.getGates()) {
            gates.add(PlanGate.builder().gateId(gs.getId()).type(gs.getType()).auto(false).afterPipelines(gs.getAfter())
                    .beforePipelines(gs.getBefore()).afterRuns(runsOf(gs.getAfter(), runOf)).beforeRuns(runsOf(gs.getBefore(), runOf))
                    .timeoutMillis(gs.getTimeoutMillis()).onTimeout(gs.getOnTimeout()).build());
        }
        Map<String, Integer> stage = new HashMap<>();
        List<PlanRun> runs = new ArrayList<>();
        for (int i = 0; i < regionList.size(); i++) {
            String runId = "R" + (i + 1);
            List<String> inGates = new ArrayList<>(), upstream = new ArrayList<>();
            for (PlanGate pg : gates) if (pg.getBeforeRuns().contains(runId)) { inGates.add(pg.getGateId()); upstream.addAll(pg.getAfterRuns()); }
            ExecMode mode = ExecMode.CLUSTER;
            for (String p : regionList.get(i)) if (modes.containsKey(p)) mode = modes.get(p);
            runs.add(PlanRun.builder().runId(runId).stage(stage(runId, gates, stage)).mode(mode).pipelineIds(regionList.get(i))
                    .inGateIds(inGates).upstreamRunIds(upstream).build());
        }
        Map<String, PlanPipeline> pipelines = new LinkedHashMap<>();
        for (String id : ids) {
            PipelineSpec spec = g.getPipelines().get(id);
            List<String> inEdges = new ArrayList<>(), ups = new ArrayList<>();
            Map<String, String> portToEdge = new LinkedHashMap<>();
            for (PlanChannel c : channels) {
                if (c.getToPipeline().equals(id)) { inEdges.add(c.getEdgeId()); ups.add(c.getFromPipeline()); }
                if (c.getFromPipeline().equals(id)) portToEdge.put(c.getFromPort(), c.getEdgeId());
            }
            pipelines.put(id, PlanPipeline.builder().pipelineId(id).runId(runOf.get(id)).source(spec.getSource() != null)
                    .parallelism(spec.getParallelism() != null ? spec.getParallelism() : 2).chunkRows(spec.getChunkRows()).chunkBytes(spec.getChunkBytes())
                    .chunkKey(spec.getChunkKey()).inputEdgeIds(inEdges).portToEdge(portToEdge).upstreamPipelines(ups).build());
        }
        int maxStage = 1;
        for (PlanRun r : runs) maxStage = Math.max(maxStage, r.getStage());
        return ExecutionPlan.builder().graphName(g.getName()).hash("test-" + g.getName()).runs(runs).gates(gates).channels(channels)
                .pipelines(pipelines).stageCount(maxStage).graph(g).build();
    }

    private static int stage(String runId, List<PlanGate> gates, Map<String, Integer> memo) {
        Integer m = memo.get(runId);
        if (m != null) return m;
        int s = 1;
        for (PlanGate g : gates) if (g.getBeforeRuns().contains(runId)) for (String a : g.getAfterRuns()) s = Math.max(s, 1 + stage(a, gates, memo));
        memo.put(runId, s);
        return s;
    }

    private static List<String> runsOf(List<String> pipelines, Map<String, String> runOf) {
        List<String> l = new ArrayList<>();
        for (String p : pipelines) if (!l.contains(runOf.get(p))) l.add(runOf.get(p));
        return l;
    }

    private static List<String> topo(PipelineGraph g, List<String> members) {
        List<String> out = new ArrayList<>(), left = new ArrayList<>(members);
        while (!left.isEmpty()) {
            for (String p : left) {
                boolean ready = true;
                for (EdgeSpec e : g.getEdges()) if (e.getToPipeline().equals(p) && left.contains(e.getFromPipeline())) ready = false;
                if (ready) { out.add(p); left.remove(p); break; }
            }
        }
        return out;
    }

    private static String find(Map<String, String> parent, String x) {
        while (!parent.get(x).equals(x)) x = parent.get(x);
        return x;
    }
}
