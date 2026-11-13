package io.github.amwtke.pipeliner.core.dataplane;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Cheap view of a stream's pending entries: the count and the oldest (lowest id) pending entry. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PendingSummary {
    private long pending;
    private String oldestEntryId;
    /** Milliseconds since the oldest pending entry was last delivered/claimed (renewals reset it). */
    private long oldestIdleMillis;
    private String oldestConsumer;
    private long oldestDeliveryCount;

    public static PendingSummary none() { return new PendingSummary(0, null, 0, null, 0); }
}
