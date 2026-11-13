package io.github.amwtke.pipeliner.api;

import java.util.Map;

/** Context for sinks. */
public interface SinkContext {
    String roundId();
    String runId();
    String pipelineId();
    Map<String, String> params();
    DataSourceRegistry dataSources();
}
