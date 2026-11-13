package io.github.amwtke.pipeliner.core.graph;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.RowRunner;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SplitSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.AccessLevel;
import lombok.Getter;

/** Declarative description of one pipeline. */
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public final class PipelineSpec {
    private final String id;
    /** Null means stream-fed. */
    private final SplitSource source;
    private final RowRunner rowRunner;
    private final BatchRunner batchRunner;
    /** Lazy form: runner class instantiated on demand through the ComponentFactory (null when an instance or none is used). */
    private final Class<? extends RowRunner> rowRunnerType;
    private final Class<? extends BatchRunner> batchRunnerType;
    /** Type-based runner only: every worker thread gets its own runner instance. */
    private final boolean perThread;
    /** Always contains "out". */
    private final Set<String> ports;
    /** Port to sinks (tee). */
    private final Map<String, List<Sink>> sinks;
    private final Integer parallelism;
    private final ExecMode mode;
    private final long estimatedRows;
    private final int chunkRows;
    private final int chunkBytes;
    /** Columns whose values must not be split across stream chunks; empty = none. */
    private final List<String> chunkKey;

    public boolean hasRunner() { return runnerKind() != null; }

    /** "row", "batch" or null. */
    public String runnerKind() {
        return rowRunner != null || rowRunnerType != null ? "row" : batchRunner != null || batchRunnerType != null ? "batch" : null;
    }

    /** Class name of the runner, for both the instance and the type form; null when there is no runner. */
    public String runnerClassName() {
        if (rowRunner != null) return rowRunner.getClass().getName();
        if (batchRunner != null) return batchRunner.getClass().getName();
        if (rowRunnerType != null) return rowRunnerType.getName();
        return batchRunnerType != null ? batchRunnerType.getName() : null;
    }

    /** Number of runner declarations (instance or type); the planner requires at most one. */
    public int runnerCount() {
        return (rowRunner != null ? 1 : 0) + (batchRunner != null ? 1 : 0) + (rowRunnerType != null ? 1 : 0) + (batchRunnerType != null ? 1 : 0);
    }

    /** True when the runner is created lazily from its type. */
    public boolean lazyRunner() { return rowRunnerType != null || batchRunnerType != null; }
}
