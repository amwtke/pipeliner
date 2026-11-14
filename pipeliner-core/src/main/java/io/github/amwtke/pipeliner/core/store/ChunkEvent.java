package io.github.amwtke.pipeliner.core.store;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Journal event describing a chunk outcome. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChunkEvent {
    /** Journal stream id, set on read. */
    private String eventId;
    /** DONE | DEAD | LOCAL_RUN_DONE | LOCAL_RUN_FAILED. */
    private String type;
    private String roundId;
    private String runId;
    private String pipelineId;
    private String chunkId;
    private String inputEdge;
    private String owner;
    private Long rowsIn;
    private Map<String, Long> rowsOut;
    private Integer costMs;
    private String error;
    /**
     * The RUN attempt (generation) that produced the event; the master drops events whose attempt differs from the run's current one.
     * It is not the per-chunk retry counter (chunk retries happen inside one run attempt and are not recorded).
     */
    private int attempt;
    private long ts;
}
