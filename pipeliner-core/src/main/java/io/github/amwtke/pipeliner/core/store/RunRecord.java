package io.github.amwtke.pipeliner.core.store;

import io.github.amwtke.pipeliner.core.graph.ExecMode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Persistent run state. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RunRecord {
    private String roundId;
    private String runId;
    private int stage;
    private ExecMode mode;
    private RunStatus status;
    private int attempt;
    private String assignee;
    private long masterEpoch;
    private Long startedAt;
    private Long finishedAt;
    private String error;
}
