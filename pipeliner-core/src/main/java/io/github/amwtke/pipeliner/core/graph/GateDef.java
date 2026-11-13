package io.github.amwtke.pipeliner.core.graph;

import io.github.amwtke.pipeliner.api.GateCondition;
import lombok.Getter;

/** Gate type plus optional condition; see {@link Gates}. */
@Getter
public final class GateDef {
    private final GateType type;
    /** Null for BARRIER and MANUAL. */
    private final GateCondition condition;

    public GateDef(GateType type, GateCondition condition) {
        this.type = type;
        this.condition = condition;
    }
}
