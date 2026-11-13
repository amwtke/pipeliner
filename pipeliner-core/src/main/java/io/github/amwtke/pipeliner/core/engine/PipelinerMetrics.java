package io.github.amwtke.pipeliner.core.engine;

/** Metrics hook; the starter provides a Micrometer implementation. */
public interface PipelinerMetrics {
    PipelinerMetrics NOOP = new PipelinerMetrics() {};

    default void chunkProcessed(String round, String run, String pipeline, long rowsIn, long rowsOut, long costMs) {}
    default void chunkRetried(String round, String pipeline) {}
    default void deadLetter(String round, String pipeline) {}
    default void busyWait(String round, String pipeline, long millis) {}
    default void channelLength(String round, String edgeId, long length) {}
    default void gateWaiting(String round, String gateId, long millis) {}
    default void leaderChanged(String podId, long epoch) {}
    default void runFinished(String round, String run, String status, long millis) {}
}
