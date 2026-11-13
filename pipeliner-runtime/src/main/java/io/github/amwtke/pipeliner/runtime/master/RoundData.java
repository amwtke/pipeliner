package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RoundView;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import java.util.HashMap;
import java.util.Map;

/** Snapshot of one round (store view + plan) with lookups by id. */
final class RoundData {
    final RoundView view;
    final ExecutionPlan plan;
    private final Map<String, RunRecord> runs = new HashMap<>();
    private final Map<String, PipelineRecord> pipes = new HashMap<>();
    private final Map<String, GateRecord> gates = new HashMap<>();
    private boolean dirty;

    RoundData(RoundView view, ExecutionPlan plan) {
        this.view = view;
        this.plan = plan;
        for (RunRecord r : view.getRuns()) runs.put(r.getRunId(), r);
        for (PipelineRecord p : view.getPipelines()) pipes.put(p.getPipelineId(), p);
        for (GateRecord g : view.getGates()) gates.put(g.getGateId(), g);
    }

    static RoundData load(MasterContext c, String roundId, ExecutionPlan plan) {
        RoundView v = c.s.store.loadRoundState(roundId);
        return v == null ? null : new RoundData(v, plan);
    }

    /** A phase wrote state that later phases depend on: the tick reloads before the next phase. */
    void touch() { dirty = true; }
    boolean dirty() { return dirty; }

    String roundId() { return view.getRound().getRoundId(); }
    RoundRecord round() { return view.getRound(); }
    RunRecord run(String id) { return runs.get(id); }
    PipelineRecord pipe(String id) { return pipes.get(id); }
    GateRecord gate(String id) { return gates.get(id); }

    boolean allSucceeded(Iterable<String> pipelineIds) {
        for (String p : pipelineIds) if (pipes.get(p) == null || pipes.get(p).getStatus() != PipelineStatus.SUCCEEDED) return false;
        return true;
    }

    boolean allRunsSucceeded(Iterable<String> runIds) {
        for (String r : runIds) if (runs.get(r) == null || runs.get(r).getStatus() != RunStatus.SUCCEEDED) return false;
        return true;
    }

    boolean allPipelinesSucceeded(PlanRun run) { return allSucceeded(run.getPipelineIds()); }
}
