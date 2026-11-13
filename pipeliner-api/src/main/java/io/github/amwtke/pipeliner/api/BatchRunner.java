package io.github.amwtke.pipeliner.api;

import java.util.List;

/** Whole-chunk processing. Same thread-safety rule as RowRunner. */
public interface BatchRunner {
    void process(List<Row> rows, Collector out, RunContext ctx) throws Exception;
}
