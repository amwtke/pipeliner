package io.github.amwtke.pipeliner.core.graph;

import io.github.amwtke.pipeliner.api.GateCondition;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.AccessLevel;
import lombok.Getter;

/** Declared gate between pipelines. */
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public final class GateSpec {
    private final String id;
    private final GateType type;
    private final GateCondition condition;
    private final List<String> after;
    private final List<String> before;
    private final long timeoutMillis;
    private final TimeoutPolicy onTimeout;
}
