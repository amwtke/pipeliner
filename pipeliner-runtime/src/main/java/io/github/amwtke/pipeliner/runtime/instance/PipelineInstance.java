package io.github.amwtke.pipeliner.runtime.instance;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.ComponentFactory;
import io.github.amwtke.pipeliner.api.Lifecycle;
import io.github.amwtke.pipeliner.api.PipelineContext;
import io.github.amwtke.pipeliner.api.RowRunner;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SplitSource;
import io.github.amwtke.pipeliner.core.graph.PipelineSpec;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * The resolved components of one pipeline on one pod: source, sinks and the runner (shared, or null when perThread;
 * then each {@link Lease} owns its runner). Lifecycle components are opened on creation and closed on release.
 */
@Slf4j
public final class PipelineInstance {
    private final PipelineSpec spec;
    private final PipelineContext ctx;
    private final ComponentFactory factory;
    private final List<Lifecycle> opened = new ArrayList<>();
    private Object runner;
    private boolean runnerFromFactory;

    PipelineInstance(PipelineSpec spec, PipelineContext ctx, ComponentFactory factory) {
        this.spec = spec;
        this.ctx = ctx;
        this.factory = factory;
    }

    public SplitSource source() { return spec.getSource(); }

    boolean perThread() { return spec.isPerThread(); }

    String runnerClass() { return spec.runnerClassName(); }

    /** Resolves the shared runner and opens runner, source and sinks; on failure closes what was opened. */
    void open() throws Exception {
        try {
            if (!spec.isPerThread()) {
                runnerFromFactory = spec.lazyRunner();
                runner = newRunner();
                openComponent(runner);
            }
            openComponent(spec.getSource());
            for (List<Sink> l : spec.getSinks().values()) for (Sink k : l) openComponent(k);
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    /** Closes (reverse order) and destroys; exceptions are logged. */
    void close() {
        for (int i = opened.size() - 1; i >= 0; i--) closeComponent(opened.get(i));
        opened.clear();
        if (runner != null && runnerFromFactory) destroy(runner);
        runner = null;
    }

    Object newRunner() {
        if (spec.getRowRunner() != null) return spec.getRowRunner();
        if (spec.getBatchRunner() != null) return spec.getBatchRunner();
        if (spec.getRowRunnerType() != null) return factory.create(spec.getRowRunnerType());
        if (spec.getBatchRunnerType() != null) return factory.create(spec.getBatchRunnerType());
        return null;
    }

    void destroy(Object component) {
        try { factory.destroy(component); }
        catch (Throwable t) { log.warn("destroy of {} failed: {}", component.getClass().getName(), t.toString()); }
    }

    private void openComponent(Object c) throws Exception {
        if (!(c instanceof Lifecycle)) return;
        ((Lifecycle) c).open(ctx);
        opened.add((Lifecycle) c);
    }

    void closeComponent(Object c) {
        if (!(c instanceof Lifecycle)) return;
        try { ((Lifecycle) c).close(); }
        catch (Throwable t) { log.warn("close of {} in pipeline {} failed: {}", c.getClass().getName(), spec.getId(), t.toString()); }
    }

    PipelineContext context() { return ctx; }

    /** One holder (worker thread or local executor) of an instance; owns its own runner when perThread. */
    public static final class Lease {
        private final PipelineInstances owner;
        private final String key;
        private final PipelineInstance instance;
        private Object ownRunner;

        Lease(PipelineInstances owner, String key, PipelineInstance instance) {
            this.owner = owner;
            this.key = key;
            this.instance = instance;
        }

        /** Creates and opens this lease's own runner (perThread only). */
        void openOwnRunner() throws Exception {
            if (!instance.perThread()) return;
            Object r = instance.newRunner();
            try {
                if (r instanceof Lifecycle) ((Lifecycle) r).open(instance.context());
            } catch (Throwable t) {
                instance.closeComponent(r);
                instance.destroy(r);
                throw t;
            }
            ownRunner = r;
        }

        public PipelineInstance instance() { return instance; }

        public RowRunner rowRunner() {
            Object r = runner();
            return r instanceof RowRunner ? (RowRunner) r : null;
        }

        public BatchRunner batchRunner() {
            Object r = runner();
            return r instanceof BatchRunner ? (BatchRunner) r : null;
        }

        private Object runner() { return instance.perThread() ? ownRunner : instance.runner; }

        /** Releases the lease: closes/destroys the own runner, and the instance when this was the last holder. */
        public void release() {
            if (ownRunner != null) {
                instance.closeComponent(ownRunner);
                instance.destroy(ownRunner);
                ownRunner = null;
            }
            owner.release(key);
        }
    }
}
