package io.github.amwtke.pipeliner.core.plan;

import lombok.Builder;
import lombok.Getter;

/** Stream channel materializing one edge. */
@Getter
@Builder
public final class PlanChannel {
    /** "e1".. */
    private final String edgeId;
    /** 1.. */
    private final int ordinal;
    private final String fromPipeline;
    private final String fromPort;
    private final String toPipeline;
    private final String runId;
}
