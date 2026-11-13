package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.SinkContext;
import java.util.Map;

/** SinkContext over round params. */
public final class SinkContextImpl implements SinkContext {
    private final String roundId, runId, pipelineId;
    private final Map<String, String> params;
    private final DataSourceRegistry dataSources;

    public SinkContextImpl(String roundId, String runId, String pipelineId, Map<String, String> params, DataSourceRegistry dataSources) {
        this.roundId = roundId;
        this.runId = runId;
        this.pipelineId = pipelineId;
        this.params = params;
        this.dataSources = dataSources;
    }

    @Override public String roundId() { return roundId; }
    @Override public String runId() { return runId; }
    @Override public String pipelineId() { return pipelineId; }
    @Override public Map<String, String> params() { return params; }
    @Override public DataSourceRegistry dataSources() { return dataSources; }
}
