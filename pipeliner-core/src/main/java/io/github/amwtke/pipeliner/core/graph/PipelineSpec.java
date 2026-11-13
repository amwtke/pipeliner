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
}
