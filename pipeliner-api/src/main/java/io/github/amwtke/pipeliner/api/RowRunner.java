package io.github.amwtke.pipeliner.api;

/** Row-at-a-time processing. Instances are shared across threads: must be stateless/thread-safe. */
public interface RowRunner {
    void process(Row row, Collector out, RunContext ctx) throws Exception;
}
