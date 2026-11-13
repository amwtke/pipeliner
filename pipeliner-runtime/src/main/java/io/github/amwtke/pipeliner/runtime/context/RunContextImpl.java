package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import io.github.amwtke.pipeliner.core.util.NamedSql;
import java.util.List;
import java.util.Map;

/** RunContext over round params and the datasource registry. */
public final class RunContextImpl implements RunContext {
    private final String roundId, runId, pipelineId, chunkId;
    private final Map<String, String> params;
    private final DataSourceRegistry dataSources;
    private final GateFallbackCache fallback;

    public RunContextImpl(String roundId, String runId, String pipelineId, String chunkId, Map<String, String> params,
                          DataSourceRegistry dataSources, GateFallbackCache fallback) {
        this.roundId = roundId;
        this.runId = runId;
        this.pipelineId = pipelineId;
        this.chunkId = chunkId;
        this.params = params;
        this.dataSources = dataSources;
        this.fallback = fallback;
    }

    @Override public String roundId() { return roundId; }
    @Override public String runId() { return runId; }
    @Override public String pipelineId() { return pipelineId; }
    @Override public String chunkId() { return chunkId; }
    @Override public Map<String, String> params() { return params; }
    @Override public String param(String name) { return params.get(name); }

    @Override public List<Row> lookup(String datasource, String sql, Map<String, ?> namedParams) {
        return NamedSql.query(dataSources.get(datasource), sql, namedParams);
    }

    @Override public boolean gateFallback(String gateId) { return fallback.isFallback(roundId, gateId); }
}
