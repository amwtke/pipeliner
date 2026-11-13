package io.github.amwtke.pipeliner.core.dataplane;

import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

/** Moves a poisoned input entry to the DLQ. */
@Data
@Builder
public class DeadLetterRequest {
    private String roundId;
    private String pipelineId;
    private String inputStreamKey;
    private String inputEntryId;
    private Map<String, String> inputFields;
    private String error;
    private ChunkEvent event;
}
