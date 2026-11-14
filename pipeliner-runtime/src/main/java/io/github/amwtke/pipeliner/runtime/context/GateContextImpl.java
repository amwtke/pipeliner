package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.GateContext;
import io.github.amwtke.pipeliner.core.store.ChunkStatus;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import java.util.Map;

/** GateContext evaluated by the master. */
public final class GateContextImpl implements GateContext {
    private final String roundId, gateId;
    private final Map<String, String> params, signals;
    private final long armedAt, now;
    private final ControlPlaneStore store;
    private final DataSourceRegistry dataSources;
    private final java.util.function.ToIntFunction<String> attemptOf;

    public GateContextImpl(String roundId, String gateId, Map<String, String> params, Map<String, String> signals,
                           long armedAt, long now, ControlPlaneStore store, DataSourceRegistry dataSources) {
        this(roundId, gateId, params, signals, armedAt, now, store, dataSources, p -> 0);
    }

    /** attemptOf maps a pipeline id to the current attempt of its run. */
    public GateContextImpl(String roundId, String gateId, Map<String, String> params, Map<String, String> signals,
                           long armedAt, long now, ControlPlaneStore store, DataSourceRegistry dataSources,
                           java.util.function.ToIntFunction<String> attemptOf) {
        this.attemptOf = attemptOf;
        this.roundId = roundId;
        this.gateId = gateId;
        this.params = params;
        this.signals = signals;
        this.armedAt = armedAt;
        this.now = now;
        this.store = store;
        this.dataSources = dataSources;
    }

    @Override public String roundId() { return roundId; }
    @Override public String gateId() { return gateId; }
    @Override public Map<String, String> params() { return params; }
    @Override public Map<String, String> signals() { return signals; }
    @Override public long armedAtMillis() { return armedAt; }
    @Override public long nowMillis() { return now; }
    @Override public long doneChunkCount(String pipelineId) { return store.countChunks(roundId, pipelineId, ChunkStatus.DONE); }
    @Override public long sinkChunkCount(String pipelineId) { return store.countSinkChunks(roundId, pipelineId); }
    @Override public DataSourceRegistry dataSources() { return dataSources; }
    @Override public int runAttemptOf(String pipelineId) { return attemptOf.applyAsInt(pipelineId); }
}
