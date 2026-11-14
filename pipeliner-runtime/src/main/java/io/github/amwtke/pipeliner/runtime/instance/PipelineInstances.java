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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/** Per-pod registry of live pipeline instances keyed by (roundId, pipelineId, attempt), reference counted by worker threads. */
@Slf4j
public final class PipelineInstances {
    private static final class Entry {
        final String roundId, pipelineId;
        final int attempt;
        final PipelineInstance instance;
        int refs;
        final CompletableFuture<Void> opened = new CompletableFuture<>();
        Entry(String roundId, String pipelineId, int attempt, PipelineInstance instance) {
            this.roundId = roundId; this.pipelineId = pipelineId; this.attempt = attempt; this.instance = instance;
        }
    }

    private final ComponentFactory factory;
    private final String podId;
    private final DataSourceRegistry dataSources;
    private final Map<String, Entry> live = new LinkedHashMap<>();
    private final SharedLifecycles shared = new SharedLifecycles();

    public PipelineInstances(ComponentFactory factory, String podId, DataSourceRegistry dataSources) {
        this.factory = factory;
        this.podId = podId;
        this.dataSources = dataSources;
    }

    /**
     * Instances are keyed by (round, pipeline, run attempt): a new attempt gets a fresh instance (open() runs again). Takes a reference,
     * creating and opening the instance when this is the first one; concurrent acquirers of the same key wait for that one open. For
     * perThread pipelines also creates the lease's own runner. Throws when open fails (nothing stays registered / referenced).
     * User open() and close() code never runs under this object's monitor, so {@link #snapshot()} (heartbeat) never blocks on it.
     */
    public PipelineInstance.Lease acquire(ExecutionPlan plan, String roundId, Map<String, String> params, String pipelineId, int attempt) throws Exception {
        String key = roundId + "|" + pipelineId + "|" + attempt;
        Entry e;
        boolean creator = false;
        synchronized (this) {
            e = live.get(key);
            if (e == null) {
                PlanPipeline pp = plan.pipeline(pipelineId);
                PipelineSpec spec = plan.getGraph().getPipelines().get(pipelineId);
                PipelineInstance inst = new PipelineInstance(spec, new PipelineContextImpl(roundId, pp.getRunId(), pipelineId, podId, params, dataSources), factory, shared);
                e = new Entry(roundId, pipelineId, attempt, inst);
                live.put(key, e);
                creator = true;
            }
            e.refs++;
        }
        if (creator) {
            try {
                e.instance.open();
            } catch (Throwable t) {
                synchronized (this) { if (live.get(key) == e) live.remove(key); }
                e.opened.completeExceptionally(t);
                throw t;
            }
            e.opened.complete(null);
            log.debug("pipeline instance {} created", key);
        } else {
            try {
                e.opened.get();
            } catch (ExecutionException ex) {   // the creator failed and removed the entry
                Throwable cause = ex.getCause();
                if (cause instanceof Exception) throw (Exception) cause;
                if (cause instanceof Error) throw (Error) cause;
                throw ex;
            } catch (InterruptedException ie) {
                release(key, e);
                throw ie;
            }
        }
        PipelineInstance.Lease lease = new PipelineInstance.Lease(this, key, e.instance);
        try {
            lease.openOwnRunner();
        } catch (Throwable t) {
            release(key);
            throw t;
        }
        return lease;
    }

    void release(String key) { release(key, null); }

    /** expected: when non-null the release only applies to that very entry (it may have been replaced after a failed open). */
    private void release(String key, Entry expected) {
        Entry e;
        synchronized (this) {
            e = live.get(key);
            if (e == null || (expected != null && e != expected) || --e.refs > 0) return;
            live.remove(key);
        }
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
