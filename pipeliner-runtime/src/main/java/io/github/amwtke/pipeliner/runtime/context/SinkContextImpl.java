package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.SinkContext;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import java.util.Map;

/** SinkContext over round params. */
public final class SinkContextImpl implements SinkContext {
    private final String roundId, runId, pipelineId;
    private final Map<String, String> params;
    private final DataSourceRegistry dataSources;
    private final int attempt;
    private final long writeVersion;

    public SinkContextImpl(String roundId, String runId, String pipelineId, Map<String, String> params, DataSourceRegistry dataSources) {
        this(roundId, runId, pipelineId, params, dataSources, 0, 0L);
    }

    public SinkContextImpl(String roundId, String runId, String pipelineId, Map<String, String> params, DataSourceRegistry dataSources,
                           int attempt, long writeVersion) {
        this.attempt = attempt;
        this.writeVersion = writeVersion;
        this.roundId = roundId;
        this.runId = runId;
        this.pipelineId = pipelineId;
        this.params = params;
        this.dataSources = dataSources;
    }

    @Override public int attempt() { return attempt; }
    @Override public long writeVersion() { return writeVersion; }
    @Override public String batchId(Chunk chunk) { return ChunkIds.batchId(roundId, pipelineId, chunk.getChunkId(), attempt); }
    @Override public String roundId() { return roundId; }
    @Override public String runId() { return runId; }
    @Override public String pipelineId() { return pipelineId; }
    @Override public Map<String, String> params() { return params; }
    @Override public DataSourceRegistry dataSources() { return dataSources; }
}
