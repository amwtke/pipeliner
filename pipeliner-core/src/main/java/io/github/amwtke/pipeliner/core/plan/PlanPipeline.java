package io.github.amwtke.pipeliner.core.plan;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/** Planned pipeline. */
@Getter
@Builder
public final class PlanPipeline {
    private final String pipelineId;
    private final String runId;
    private final boolean source;
    private final int parallelism;
    private final int chunkRows;
    private final int chunkBytes;
    /** Empty = none; see PipelineSpec.getChunkKey. */
    private final List<String> chunkKey;
    private final List<String> inputEdgeIds;
    /** Stream ports to edge ids. */
    private final Map<String, String> portToEdge;
    private final List<String> upstreamPipelines;
}
