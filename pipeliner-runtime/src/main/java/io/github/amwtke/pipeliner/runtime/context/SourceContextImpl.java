package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.SourceContext;
import java.util.Map;

/** SourceContext over round params. */
public final class SourceContextImpl implements SourceContext {
    private final String roundId;
    private final Map<String, String> params;
    private final DataSourceRegistry dataSources;

    public SourceContextImpl(String roundId, Map<String, String> params, DataSourceRegistry dataSources) {
        this.roundId = roundId;
        this.params = params;
        this.dataSources = dataSources;
    }

    @Override public String roundId() { return roundId; }
    @Override public Map<String, String> params() { return params; }
    @Override public DataSourceRegistry dataSources() { return dataSources; }
}
