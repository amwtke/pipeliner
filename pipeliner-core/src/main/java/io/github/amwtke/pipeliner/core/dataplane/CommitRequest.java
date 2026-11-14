package io.github.amwtke.pipeliner.core.dataplane;

import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import java.util.List;
import lombok.Builder;
import lombok.Data;

/** Atomic ack-input + publish-outputs + journal request. */
@Data
@Builder
public class CommitRequest {
    private String roundId;
    private String inputStreamKey;
    private String inputEntryId;
    private long highWatermark;
    private List<OutputChunk> outputs;
    private ChunkEvent event;
    /** Committing consumer: the commit succeeds only while this consumer still owns the pending entry (null = no ownership check). */
    private String consumer;
}
