package io.github.amwtke.pipeliner.api;

import java.util.Map;

/** Context for sinks. */
public interface SinkContext {
    String roundId();
    String runId();
    String pipelineId();
    Map<String, String> params();
    DataSourceRegistry dataSources();
    /** Run attempt this write belongs to (increments on every rerun of the run). */
    default int attempt() { return 0; }
    /** Globally monotonic version of this write: roundSeq * 1000 + attempt. A newer rerun/round always has a higher value. */
    default long writeVersion() { return 0L; }
}
