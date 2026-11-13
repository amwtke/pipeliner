package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.graph.PipelineSpec;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.plan.PlanGate;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builds the structured plan JSON served to the UI from an {@link ExecutionPlan} and its live graph. */
final class PlanViews {
    private PlanViews() {}

    static Map<String, Object> plan(ExecutionPlan plan) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("graphName", plan.getGraphName());
        out.put("hash", plan.getHash());
        out.put("stageCount", plan.getStageCount());
        List<Object> runs = new ArrayList<>();
        for (PlanRun r : plan.getRuns()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("runId", r.getRunId());
            m.put("stage", r.getStage());
            m.put("mode", r.getMode() == null ? null : r.getMode().name());
            m.put("pipelineIds", r.getPipelineIds());
            m.put("inGateIds", r.getInGateIds());
            m.put("upstreamRunIds", r.getUpstreamRunIds());
            runs.add(m);
        }
        out.put("runs", runs);
        PipelineGraph g = plan.getGraph();
        List<Object> pls = new ArrayList<>();
        for (PlanPipeline p : plan.getPipelines().values()) {
            PipelineSpec spec = g == null ? null : g.getPipelines().get(p.getPipelineId());
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", p.getPipelineId());
            m.put("runId", p.getRunId());
            m.put("source", spec == null || spec.getSource() == null ? null : typed(spec.getSource().getClass(), spec.getSource().datasetName()));
            Map<String, Object> runner = new LinkedHashMap<>();
            String cn = spec == null ? null : spec.runnerClassName();
            runner.put("class", cn == null ? null : simple(cn));
            runner.put("kind", spec == null || spec.runnerKind() == null ? "none" : spec.runnerKind());
            runner.put("perThread", spec != null && spec.isPerThread());
            m.put("runner", runner);
            m.put("ports", spec == null ? new ArrayList<>() : new ArrayList<>(spec.getPorts()));
            Map<String, Object> sinks = new LinkedHashMap<>();
            if (spec != null) for (Map.Entry<String, List<Sink>> e : spec.getSinks().entrySet()) {
                List<Object> l = new ArrayList<>();
                for (Sink s : e.getValue()) l.add(typed(s.getClass(), s.datasetName()));
                sinks.put(e.getKey(), l);
            }
            m.put("sinks", sinks);
            m.put("parallelism", p.getParallelism());
            m.put("chunkKey", p.getChunkKey());
            m.put("inputEdgeIds", p.getInputEdgeIds());
            m.put("portToEdge", p.getPortToEdge());
            pls.add(m);
        }
        out.put("pipelines", pls);
        List<Object> chs = new ArrayList<>();
        for (PlanChannel c : plan.getChannels()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("edgeId", c.getEdgeId());
            m.put("ordinal", c.getOrdinal());
            m.put("from", c.getFromPipeline());
            m.put("fromPort", c.getFromPort());
            m.put("to", c.getToPipeline());
            m.put("runId", c.getRunId());
            chs.add(m);
        }
        out.put("channels", chs);
        List<Object> gates = new ArrayList<>();
        for (PlanGate gt : plan.getGates()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("gateId", gt.getGateId());
            m.put("type", gt.getType() == null ? null : gt.getType().name());
            m.put("auto", gt.isAuto());
            m.put("dataset", gt.getDataset());
            m.put("afterPipelines", gt.getAfterPipelines());
            m.put("beforePipelines", gt.getBeforePipelines());
            m.put("afterRuns", gt.getAfterRuns());
            m.put("beforeRuns", gt.getBeforeRuns());
            m.put("timeoutMillis", gt.getTimeoutMillis());
            m.put("onTimeout", gt.getOnTimeout() == null ? null : gt.getOnTimeout().name());
            gates.add(m);
        }
        out.put("gates", gates);
        return out;
    }

    private static Map<String, Object> typed(Class<?> c, String dataset) {
        Map<String, Object> m = new LinkedHashMap<>();
        String n = c.getSimpleName();
        m.put("type", n.isEmpty() ? simple(c.getName()) : n);
        m.put("dataset", dataset);
        return m;
    }

    private static String simple(String cn) {
        int i = Math.max(cn.lastIndexOf('.'), cn.lastIndexOf('$'));
        String s = i < 0 ? cn : cn.substring(i + 1);
        return s.isEmpty() ? cn : s;
    }
}
