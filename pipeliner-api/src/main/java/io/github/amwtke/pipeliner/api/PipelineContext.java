package io.github.amwtke.pipeliner.api;

import java.util.Map;

/** Context of one pipeline activation on a pod (handed to {@link Lifecycle#open}). */
public interface PipelineContext {
    String roundId();
    String runId();
    String pipelineId();
    /** The pod this activation lives on. */
    String podId();
    /** Round params, e.g. planVersion. */
    Map<String, String> params();
    DataSourceRegistry dataSources();
}
