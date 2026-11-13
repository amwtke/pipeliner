package io.github.amwtke.pipeliner.api;

/**
 * Optional for runners, sources and sinks. open() runs when a pipeline is activated on a pod (before its first chunk),
 * close() when the last thread of that pipeline on the pod has finished. Components registered as instances (not as types)
 * are shared singletons but are still opened/closed on every activation: stateful components should use the type-based form.
 */
public interface Lifecycle {
    default void open(PipelineContext ctx) throws Exception {}
    default void close() throws Exception {}
}
