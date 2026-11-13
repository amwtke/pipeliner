package io.github.amwtke.pipeliner.core.engine;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** A pipeline currently instantiated on this pod. */
@Getter
@AllArgsConstructor
public final class LiveInstance {
    private final String roundId;
    private final String pipelineId;
    /** Number of worker threads (or local executors) currently holding the instance. */
    private final int threads;
    private final boolean perThread;
    /** Runner class name, null when the pipeline has no runner. */
    private final String runner;
    /** Run attempt the instance belongs to. */
    private final int attempt;

    public LiveInstance(String roundId, String pipelineId, int threads, boolean perThread, String runner) {
        this(roundId, pipelineId, threads, perThread, runner, 0);
    }
}
