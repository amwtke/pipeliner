package io.github.amwtke.pipeliner.core.dataplane;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

/** One stream entry; fields: "c" = chunk json, "s" = split json. */
@Data
@AllArgsConstructor
public class StreamEntry {
    private String streamKey;
    private String entryId;
    private Map<String, String> fields;
    /** Times the entry was delivered (XPENDING); filled by autoClaim only, 0 otherwise. */
    private long deliveryCount;

    public StreamEntry(String streamKey, String entryId, Map<String, String> fields) { this(streamKey, entryId, fields, 0); }
}
