package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.PipelineContext;
import java.util.Map;

public final class PipelineContextImpl implements PipelineContext {
    private final String roundId, runId, pipelineId, podId;
    private final Map<String, String> params;
    private final DataSourceRegistry dataSources;

    public PipelineContextImpl(String roundId, String runId, String pipelineId, String podId, Map<String, String> params, DataSourceRegistry dataSources) {
        this.roundId = roundId;
        this.runId = runId;
        this.pipelineId = pipelineId;
        this.podId = podId;
        this.params = params;
        this.dataSources = dataSources;
    }

    @Override public String roundId() { return roundId; }
    @Override public String runId() { return runId; }
    @Override public String pipelineId() { return pipelineId; }
    @Override public String podId() { return podId; }
    @Override public Map<String, String> params() { return params; }
    @Override public DataSourceRegistry dataSources() { return dataSources; }
}
