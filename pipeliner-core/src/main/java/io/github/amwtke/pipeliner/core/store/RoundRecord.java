package io.github.amwtke.pipeliner.core.store;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Persistent round state. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RoundRecord {
    private String roundId;
    private String graphName;
    private String bizKey;
    private String planHash;
    private String planJson;
    private Map<String, String> params;
    private RoundStatus status;
    private long masterEpoch;
    @Builder.Default
    private String journalOffset = "0-0";
    private Long createdAt;
    private Long startedAt;
    private Long finishedAt;
    private String error;
    private boolean incomplete;
    /** Globally monotonic submit sequence (DataPlane.nextRoundSeq); the base of sink write versions. */
    private long seq;
}
