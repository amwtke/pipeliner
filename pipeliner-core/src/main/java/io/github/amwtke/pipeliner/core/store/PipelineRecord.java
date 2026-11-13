package io.github.amwtke.pipeliner.core.store;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Persistent pipeline state. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PipelineRecord {
    private String roundId;
    private String pipelineId;
    private String runId;
    private PipelineStatus status;
    private int parallelism;
    private boolean enumDone;
    private long masterEpoch;
    private Long startedAt;
    private Long finishedAt;
    private String error;
}
