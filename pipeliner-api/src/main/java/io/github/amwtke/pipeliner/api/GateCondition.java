package io.github.amwtke.pipeliner.api;

/** Decides whether a gate opens. */
public interface GateCondition {
    GateDecision evaluate(GateContext ctx) throws Exception;
}
