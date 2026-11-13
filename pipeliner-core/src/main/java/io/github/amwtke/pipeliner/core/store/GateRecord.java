package io.github.amwtke.pipeliner.core.store;

import io.github.amwtke.pipeliner.core.graph.GateType;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Persistent gate state. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GateRecord {
    private String roundId;
    private String gateId;
    private GateType type;
    private List<String> beforeRuns;
    private GateStatus status;
    private Long armedAt;
    private Long deadlineAt;
    private String openedBy;
    private Long openedAt;
    private Map<String, String> context;
    private long masterEpoch;
    private boolean alerted;
}
