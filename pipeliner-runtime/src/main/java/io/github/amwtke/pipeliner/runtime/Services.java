package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.api.ComponentFactory;
import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.core.dataplane.DataPlane;
import io.github.amwtke.pipeliner.core.engine.PipelinerMetrics;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.context.GateFallbackCache;
import io.github.amwtke.pipeliner.runtime.instance.PipelineInstances;

/** Shared collaborators handed to every runtime component. */
public final class Services {
    public final RuntimeConfig cfg;
    public final ControlPlaneStore store;
    public final DataPlane plane;
    public final DataSourceRegistry dataSources;
    public final PipelinerMetrics metrics;
    public final GraphRegistry registry;
    public final GateFallbackCache fallback;
    /** Pipelines instantiated on this pod. */
    public final PipelineInstances instances;

    public Services(RuntimeConfig cfg, ControlPlaneStore store, DataPlane plane, DataSourceRegistry dataSources,
                    PipelinerMetrics metrics, GraphRegistry registry, ComponentFactory factory) {
        this.cfg = cfg;
        this.store = store;
        this.plane = plane;
        this.dataSources = dataSources;
        this.metrics = metrics == null ? PipelinerMetrics.NOOP : metrics;
        this.registry = registry;
        this.fallback = new GateFallbackCache(store);
        this.instances = new PipelineInstances(factory, cfg.getPodId(), dataSources);
    }

    public String podId() { return cfg.getPodId(); }
}
