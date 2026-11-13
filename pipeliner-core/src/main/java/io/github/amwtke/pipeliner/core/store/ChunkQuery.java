package io.github.amwtke.pipeliner.core.store;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Chunk listing filter; null filters are ignored. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChunkQuery {
    private String roundId;
    private String pipelineId;
    private String runId;
    private ChunkStatus status;
    @Builder.Default
    private int limit = 1000;
    @Builder.Default
    private int offset = 0;
}
