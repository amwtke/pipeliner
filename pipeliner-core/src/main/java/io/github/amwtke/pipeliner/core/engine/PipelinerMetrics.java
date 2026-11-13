package io.github.amwtke.pipeliner.core.engine;

/**
 * Metrics hook; the starter provides a Micrometer implementation. Round-scoped hooks carry the round id; implementations must NOT use it as a
 * tag of counters/timers (unbounded cardinality) and must drop round-tagged gauges on {@link #roundFinished}.
 */
public interface PipelinerMetrics {
    PipelinerMetrics NOOP = new PipelinerMetrics() {};

    default void chunkProcessed(String round, String run, String pipeline, long rowsIn, long rowsOut, long costMs) {}
    default void chunkRetried(String round, String pipeline) {}
    default void deadLetter(String round, String pipeline) {}
    default void busyWait(String round, String pipeline, long millis) {}
    default void channelLength(String round, String edgeId, long length) {}
    default void gateWaiting(String round, String gateId, long millis) {}
    /** A gate hit its deadline; policy is the TimeoutPolicy name. */
    default void gateTimeout(String graph, String gateId, String policy) {}
    /** A graph was registered on this pod: lets implementations create zero-valued series (so increase() sees the first event). */
    default void graphRegistered(String graph, java.util.Collection<String> pipelineIds, java.util.Collection<String> gateIds) {}
    default void leaderChanged(String podId, long epoch) {}
    /** This pod is no longer the leader. */
    default void leaderLost(String podId) {}
    /** A leader tick completed (LOCAL thread or EXTERNAL trigger) and took this long. */
    default void masterTick(long durationMillis) {}
    default void runFinished(String round, String run, String status, long millis) {}
    /** The round reached a terminal status (SUCCEEDED, FAILED, CANCELLED). */
    default void roundFinished(String graph, String round, String status) {}
    /** Number of rounds in the active set, sampled by the leader. */
    default void activeRounds(int count) {}
    /** Pipeline instances currently alive on this pod. */
    default void liveInstances(String podId, int count) {}
    /** Configured channel high watermark (for backpressure alerts). */
    default void highWatermark(int value) {}
    /** Idle time of the oldest pending entry of the pipeline's input streams (seconds-resolution gauge). */
    default void oldestPending(String round, String pipeline, long idleMillis) {}
    /** A pending entry exceeded stalledChunkAlertMillis (once per entry). */
    default void stalledChunk(String graph, String pipeline) {}
    default void redisUsedMemory(long bytes) {}
    /** One retention pass that did work or ran (counts may be 0). */
    default void retentionPass(int compactedRounds, int purgedRounds, int prunedWorkers) {}
}
