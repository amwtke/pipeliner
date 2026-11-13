package io.github.amwtke.pipeliner.api;

/** Terminal writer. write() MUST be idempotent for the same chunk (re-invoked on replay); chunk ids are stable across replays. */
public interface Sink {
    /** Nullable. */
    String datasetName();
    void write(Chunk chunk, SinkContext ctx) throws Exception;
}
