package io.github.amwtke.pipeliner.core.plan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

/** Immutable result of planning: runs, gates, channels and the live graph. */
@Getter
@Builder
public final class ExecutionPlan {
    private final String graphName;
    private final String hash;
    private final List<PlanRun> runs;
    private final List<PlanGate> gates;
    private final List<PlanChannel> channels;
    private final Map<String, PlanPipeline> pipelines;
    private final int stageCount;
    /** Live graph (runner/source/sink instances); not serialized. */
    @JsonIgnore
    private final PipelineGraph graph;

    public PlanRun run(String runId) { return find(runs, runId); }

    public PlanPipeline pipeline(String id) { return pipelines.get(id); }

    public PlanGate gate(String id) {
        for (PlanGate g : gates) if (g.getGateId().equals(id)) return g;
        return null;
    }

    public PlanChannel channel(String edgeId) {
        for (PlanChannel c : channels) if (c.getEdgeId().equals(edgeId)) return c;
        return null;
    }

    public List<PlanChannel> inputsOf(String pipelineId) {
        List<PlanChannel> l = new ArrayList<>();
        for (PlanChannel c : channels) if (c.getToPipeline().equals(pipelineId)) l.add(c);
        return l;
    }

    public List<PlanChannel> outputsOf(String pipelineId) {
        List<PlanChannel> l = new ArrayList<>();
        for (PlanChannel c : channels) if (c.getFromPipeline().equals(pipelineId)) l.add(c);
        return l;
    }

    /** Transitive downstream runs (via gates), in plan order. */
    public List<String> downstreamRuns(String runId) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> todo = new ArrayDeque<>();
        todo.add(runId);
        while (!todo.isEmpty()) {
            String cur = todo.poll();
            for (PlanGate g : gates) {
                if (!g.getAfterRuns().contains(cur)) continue;
                for (String b : g.getBeforeRuns()) if (!b.equals(runId) && seen.add(b)) todo.add(b);
            }
        }
        List<String> out = new ArrayList<>();
        for (PlanRun r : runs) if (seen.contains(r.getRunId())) out.add(r.getRunId());
        return out;
    }

    /** Human-readable plan: header, then per stage its runs and the gates that open the next stage. */
    public String explain() {
        StringBuilder sb = new StringBuilder();
        String h = hash == null ? "" : hash.substring(0, Math.min(8, hash.length()));
        sb.append("ExecutionPlan ").append(graphName).append("  hash=").append(h).append("...  ")
                .append(runs.size()).append(" runs / ").append(gates.size()).append(" gates / ").append(pipelines.size()).append(" pipelines\n");
        for (int st = 1; st <= stageCount; st++) {
            boolean first = true;
            for (PlanRun r : runs) {
                if (r.getStage() != st) continue;
                sb.append(first ? "Stage " + st + "  " : "         ").append(String.format(java.util.Locale.ROOT, "%-3s %-8s ", r.getRunId(), r.getMode()))
                        .append(describe(r)).append('\n');
                first = false;
            }
            for (PlanGate g : gates) {
                int max = 0;
                for (String ar : g.getAfterRuns()) max = Math.max(max, run(ar).getStage());
                if (max != st) continue;
                sb.append("         ").append(String.format(java.util.Locale.ROOT, "%-20s %-9s ", g.getGateId(), g.getType().name().toLowerCase(java.util.Locale.ROOT)));
                if (g.isAuto()) sb.append("dataset ").append(g.getDataset()).append("; ");
                sb.append("after ").append(g.getAfterPipelines()).append(" -> before ").append(g.getBeforePipelines());
                if (g.getTimeoutMillis() > 0) sb.append("; timeout ").append(g.getTimeoutMillis()).append("ms ").append(g.getOnTimeout());
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    private String describe(PlanRun r) {
        List<String> parts = new ArrayList<>();
        Set<String> inEdge = new LinkedHashSet<>();
        List<List<String>> chains = new ArrayList<>();
        List<PlanChannel> used = new ArrayList<>();
        for (PlanChannel c : channels) {
            if (!r.getRunId().equals(c.getRunId())) continue;
            List<String> last = chains.isEmpty() ? null : chains.get(chains.size() - 1);
            if (last != null && last.get(last.size() - 1).equals(c.getFromPipeline()) && !last.contains(c.getToPipeline())) {
                last.add(c.getToPipeline());
            } else {
                List<String> ch = new ArrayList<>();
                ch.add(c.getFromPipeline());
                ch.add(c.getToPipeline());
                chains.add(ch);
            }
            inEdge.add(c.getFromPipeline());
            inEdge.add(c.getToPipeline());
        }
        for (List<String> ch : chains) parts.add(String.join(" \u2192 ", ch));
        for (String p : r.getPipelineIds()) if (!inEdge.contains(p)) parts.add(p);
        return String.join(" ; ", parts);
    }

    public String toJson() { return ChunkCodec.toJson(this); }

    private static PlanRun find(List<PlanRun> runs, String id) {
        for (PlanRun r : runs) if (r.getRunId().equals(id)) return r;
        return null;
    }
}
