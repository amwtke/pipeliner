package io.github.amwtke.pipeliner.core.store;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Persistent chunk state. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChunkRecord {
    private String roundId;
    private String pipelineId;
    private String chunkId;
    private String runId;
    private ChunkStatus status;
    private int attempt;
    private String owner;
    /** Edge id or "split". */
    private String inputEdge;
    /** JSON, source chunks only. */
    private String splitSpec;
    private Long rowsIn;
    /** Edge id or "sink:<port>" to rows. */
    private Map<String, Long> rowsOut;
    private Integer costMs;
    private String error;
    private Long updatedAt;
}
