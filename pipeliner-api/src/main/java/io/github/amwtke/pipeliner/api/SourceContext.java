package io.github.amwtke.pipeliner.api;

import java.util.Map;

/** Context for bounded sources. */
public interface SourceContext {
    String roundId();
    Map<String, String> params();
    DataSourceRegistry dataSources();
}
