package io.github.amwtke.pipeliner.runtime.process;

import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import java.util.ArrayList;
import java.util.List;

/** The single place that gives enumerated splits their deterministic chunk ids (identical in every execution mode). */
public final class SplitIds {
    private SplitIds() {}

    /** The splits with ids S0000, S0001, ... in enumeration order; null means no splits. */
    public static List<Split> assign(List<Split> raw) {
        List<Split> out = new ArrayList<>();
        if (raw != null) for (Split sp : raw) out.add(sp.withId(ChunkIds.source(out.size())));
        return out;
    }
}
