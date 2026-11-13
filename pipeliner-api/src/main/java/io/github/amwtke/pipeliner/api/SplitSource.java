package io.github.amwtke.pipeliner.api;

import java.util.List;

/** Bounded source: master enumerates, workers read. enumerate() must be deterministic for the same snapshot. */
public interface SplitSource {
    /** Nullable; used for materialized-edge detection. */
    String datasetName();
    /** Split ids may be null; the framework assigns them. */
    List<Split> enumerate(SourceContext ctx) throws Exception;
    List<Row> read(Split split, SourceContext ctx) throws Exception;
}
