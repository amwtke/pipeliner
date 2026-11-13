package io.github.amwtke.pipeliner.core.plan;

import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.graph.TimeoutPolicy;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

/** Planned gate (declared or auto-generated barrier). */
@Getter
@Builder
public final class PlanGate {
    private final String gateId;
    private final GateType type;
    private final boolean auto;
    /** Auto barrier only. */
    private final String dataset;
    private final List<String> afterPipelines;
    private final List<String> afterRuns;
    private final List<String> beforePipelines;
    private final List<String> beforeRuns;
    private final long timeoutMillis;
    private final TimeoutPolicy onTimeout;
}
