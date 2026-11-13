package io.github.amwtke.pipeliner.api;

/** Sink for runner output rows. */
public interface Collector {
    /** Emits to port "out". */
    void emit(Row row);

    void emit(String port, Row row);
}
