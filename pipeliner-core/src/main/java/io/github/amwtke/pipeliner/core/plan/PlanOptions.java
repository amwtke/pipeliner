package io.github.amwtke.pipeliner.core.plan;

import io.github.amwtke.pipeliner.core.graph.ExecMode;
import lombok.Data;

/** Planner thresholds and overrides. */
@Data
public final class PlanOptions {
    private long inlineMaxRows = 50_000;
    private long localMaxRows = 2_000_000;
    private int defaultParallelism = 2;
    private ExecMode forceMode;

    public static PlanOptions defaults() { return new PlanOptions(); }
}
