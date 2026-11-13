package io.github.amwtke.pipeliner.api;

import java.util.List;
import java.util.Map;

/** Per-chunk context handed to runners. */
public interface RunContext {
    String roundId();
    String runId();
    String pipelineId();
    String chunkId();
    /** Round params, e.g. planVersion. */
    Map<String, String> params();
    String param(String name);
    /** Run a query on a named datasource. ":name" placeholders; Collection values expand to IN lists. */
    List<Row> lookup(String datasource, String sql, Map<String, ?> namedParams);
    /** True if gate gateId was opened by OPEN_WITH_FALLBACK timeout policy. */
    boolean gateFallback(String gateId);
}
