package io.github.amwtke.pipeliner.core.plan;

import io.github.amwtke.pipeliner.core.graph.ExecMode;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

/** Planned run: pipelines connected by stream edges, executed together. */
@Getter
@Builder
public final class PlanRun {
    /** "R1".. */
    private final String runId;
    /** 1.. */
    private final int stage;
    private final ExecMode mode;
    /** Topological order. */
    private final List<String> pipelineIds;
    private final List<String> inGateIds;
    private final List<String> upstreamRunIds;
}
