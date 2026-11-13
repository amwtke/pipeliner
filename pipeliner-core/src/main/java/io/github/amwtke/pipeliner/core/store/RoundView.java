package io.github.amwtke.pipeliner.core.store;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Aggregated read model of a round. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RoundView {
    private RoundRecord round;
    private List<RunRecord> runs;
    private List<GateRecord> gates;
    private List<PipelineRecord> pipelines;
    /** pipelineId to status to count. */
    private Map<String, Map<ChunkStatus, Long>> chunkCounts;
    private Map<String, Long> rowsIn;
    /** Per pipeline, summed over chunks. */
    private Map<String, Long> rowsOut;
}
