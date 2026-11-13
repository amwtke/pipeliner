package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanOptions;
import io.github.amwtke.pipeliner.core.plan.Planner;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Registered graphs and their execution plans. */
public final class GraphRegistry {
    private final Map<String, ExecutionPlan> plans = new ConcurrentHashMap<>();
    private final PlanOptions options;

    public GraphRegistry(PlanOptions options) { this.options = options; }

    public ExecutionPlan register(PipelineGraph graph) {
        ExecutionPlan plan = new Planner(options).plan(graph);
        plans.put(graph.getName(), plan);
        return plan;
    }

    /** Registers an already built plan (its graph must be live). */
    public void registerPlan(ExecutionPlan plan) { plans.put(plan.getGraphName(), plan); }

    /** Null when unknown. */
    public ExecutionPlan find(String graphName) { return plans.get(graphName); }

    public ExecutionPlan require(String graphName) {
        ExecutionPlan p = plans.get(graphName);
        if (p == null) throw new PipelinerException("unknown graph: " + graphName);
        return p;
    }

    public Collection<String> names() { return plans.keySet(); }
}
