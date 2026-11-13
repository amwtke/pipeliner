package io.github.amwtke.pipeliner.runtime.instance;

import io.github.amwtke.pipeliner.api.ComponentFactory;
import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.core.engine.LiveInstance;
import io.github.amwtke.pipeliner.core.graph.PipelineSpec;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.runtime.context.PipelineContextImpl;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Per-pod registry of live pipeline instances keyed by (roundId, pipelineId, attempt), reference counted by worker threads. */
@Slf4j
public final class PipelineInstances {
    private static final class Entry {
        final String roundId, pipelineId;
        final int attempt;
        final PipelineInstance instance;
        int refs;
        Entry(String roundId, String pipelineId, int attempt, PipelineInstance instance) {
            this.roundId = roundId; this.pipelineId = pipelineId; this.attempt = attempt; this.instance = instance;
        }
    }

    private final ComponentFactory factory;
    private final String podId;
    private final DataSourceRegistry dataSources;
    private final Map<String, Entry> live = new LinkedHashMap<>();

    public PipelineInstances(ComponentFactory factory, String podId, DataSourceRegistry dataSources) {
        this.factory = factory;
        this.podId = podId;
        this.dataSources = dataSources;
    }

    /**
     * Takes a reference, creating and opening the instance when this is the first one. For perThread pipelines also creates the
     * lease's own runner. Throws when open fails (nothing stays registered / referenced).
     */
    public PipelineInstance.Lease acquire(ExecutionPlan plan, String roundId, Map<String, String> params, String pipelineId) throws Exception {
        return acquire(plan, roundId, params, pipelineId, 0);
    }

    /** Instances are keyed by (round, pipeline, run attempt): a new attempt gets a fresh instance (open() runs again). */
    public PipelineInstance.Lease acquire(ExecutionPlan plan, String roundId, Map<String, String> params, String pipelineId, int attempt) throws Exception {
        String key = roundId + "|" + pipelineId + "|" + attempt;
        PipelineInstance.Lease lease;
        synchronized (this) {
            Entry e = live.get(key);
            if (e == null) {
                PlanPipeline pp = plan.pipeline(pipelineId);
                PipelineSpec spec = plan.getGraph().getPipelines().get(pipelineId);
                PipelineInstance inst = new PipelineInstance(spec, new PipelineContextImpl(roundId, pp.getRunId(), pipelineId, podId, params, dataSources), factory);
                inst.open();
                e = new Entry(roundId, pipelineId, attempt, inst);
                live.put(key, e);
                log.debug("pipeline instance {} created", key);
            }
            e.refs++;
            lease = new PipelineInstance.Lease(this, key, e.instance);
        }
        try {
            lease.openOwnRunner();
        } catch (Throwable t) {
            release(key);
            throw t;
        }
        return lease;
    }

    synchronized void release(String key) {
        Entry e = live.get(key);
        if (e == null || --e.refs > 0) return;
        live.remove(key);
        e.instance.close();
        log.debug("pipeline instance {} released", key);
    }

    public synchronized List<LiveInstance> snapshot() {
        List<LiveInstance> l = new ArrayList<>();
        for (Entry e : live.values())
            l.add(new LiveInstance(e.roundId, e.pipelineId, e.refs, e.instance.perThread(), e.instance.runnerClass(), e.attempt));
        return l;
    }
}
