package io.github.amwtke.pipeliner.runtime.process;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Outcome of processing one input chunk: row counts and the chunks to publish on stream edges. */
public final class ChunkResult {
    /** One output chunk bound for a stream edge. */
    public static final class EdgeChunk {
        public final PlanChannel channel;
        public final Chunk chunk;
        EdgeChunk(PlanChannel channel, Chunk chunk) { this.channel = channel; this.chunk = chunk; }
    }

    public long rowsIn;
    /** Edge id or "sink:&lt;port&gt;" to rows. */
    public final Map<String, Long> rowsOut = new LinkedHashMap<>();
    public final List<EdgeChunk> outputs = new ArrayList<>();
    public long costMillis;
}
