package io.github.amwtke.pipeliner.core.plan;

import java.util.Collection;

/** Optional extension for a SplitSource (or its runner) that also depends on further datasets; the planner adds barriers for them. */
public interface ReadsDatasets {
    Collection<String> readDatasets();
}
