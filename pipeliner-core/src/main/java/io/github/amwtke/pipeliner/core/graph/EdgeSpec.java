package io.github.amwtke.pipeliner.core.graph;

import lombok.AllArgsConstructor;
import lombok.AccessLevel;
import lombok.Getter;

/** Stream edge between a pipeline output port and a downstream pipeline. */
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public final class EdgeSpec {
    private final String fromPipeline;
    private final String fromPort;
    private final String toPipeline;
}
