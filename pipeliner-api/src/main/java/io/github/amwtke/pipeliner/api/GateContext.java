package io.github.amwtke.pipeliner.api;

import java.util.Map;

/** Context for gate condition evaluation. */
public interface GateContext {
    String roundId();
    String gateId();
    Map<String, String> params();
    /** signalKey to payload received via signalGate. */
    Map<String, String> signals();
    long armedAtMillis();
    long nowMillis();
    /** DONE chunks of a pipeline in this round. */
    long doneChunkCount(String pipelineId);
    DataSourceRegistry dataSources();
    /** Current attempt of the run that owns the pipeline (external signals can be scoped to it). */
    default int runAttemptOf(String pipelineId) { return 0; }
}
